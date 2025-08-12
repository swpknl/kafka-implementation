package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

const (
	apiKeyApiVersions  = int16(18)
	maxSupportedAPIVer = int16(4)

	errNone           = int16(0)
	errUnsupportedVer = int16(35) // Kafka UNSUPPORTED_VERSION
)

// ----- cursor helpers -----
type cursor struct {
	b   []byte
	off int
}

func (c *cursor) need(n int) error {
	if c.off+n > len(c.b) {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (c *cursor) i16() (int16, error) {
	if err := c.need(2); err != nil {
		return 0, err
	}
	v := int16(binary.BigEndian.Uint16(c.b[c.off:]))
	c.off += 2
	return v, nil
}
func (c *cursor) i32() (int32, error) {
	if err := c.need(4); err != nil {
		return 0, err
	}
	v := int32(binary.BigEndian.Uint32(c.b[c.off:]))
	c.off += 4
	return v, nil
}

// Legacy STRING (nullable): int16 length; -1 = null
func (c *cursor) str16() (string, error) {
	l, err := c.i16()
	if err != nil {
		return "", err
	}
	if l < 0 {
		return "", nil
	}
	if err := c.need(int(l)); err != nil {
		return "", err
	}
	s := string(c.b[c.off : c.off+int(l)])
	c.off += int(l)
	return s, nil
}

// Uvarint for compact (flexible) encodings
func (c *cursor) uvarint() (uint64, error) {
	v, n := binary.Uvarint(c.b[c.off:])
	if n <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	c.off += n
	return v, nil
}

// Flexible COMPACT_NULLABLE_STRING: uvarint(len+1); 0 = null
func (c *cursor) compactNullableString() (string, error) {
	n1, err := c.uvarint()
	if err != nil {
		return "", err
	}
	if n1 == 0 {
		return "", nil
	}
	n := int(n1 - 1)
	if err := c.need(n); err != nil {
		return "", err
	}
	s := string(c.b[c.off : c.off+n])
	c.off += n
	return s, nil
}

// Flexible tagged fields: count (uvarint), then {tagID uvarint, size uvarint, payload[size]}*
func (c *cursor) skipTagged() error {
	cnt, err := c.uvarint()
	if err != nil {
		return err
	}
	for i := uint64(0); i < cnt; i++ {
		if _, err := c.uvarint(); err != nil { // tag id
			return err
		}
		sz, err := c.uvarint()
		if err != nil {
			return err
		}
		if err := c.need(int(sz)); err != nil {
			return err
		}
		c.off += int(sz)
	}
	return nil
}

// ----- main server -----
func main() {
	fmt.Println("Listening on 0.0.0.0:9092 ...")
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to bind:", err)
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Fprintln(os.Stderr, "Accept error:", err)
			continue
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	lenBuf := make([]byte, 4)

	for {
		// 1) Read 4-byte frame length
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			// EOF ends the loop; other errors close the conn
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				fmt.Fprintln(os.Stderr, "Read length error:", err)
			}
			return
		}
		frameSize := int32(binary.BigEndian.Uint32(lenBuf))
		if frameSize < 0 {
			fmt.Fprintln(os.Stderr, "Negative frame size:", frameSize)
			return
		}
		// (Optional) sanity cap to avoid absurd allocations
		if frameSize > 10*1024*1024 {
			fmt.Fprintln(os.Stderr, "Frame too large:", frameSize)
			return
		}

		// 2) Read exactly 'frameSize' bytes of payload
		payload := make([]byte, frameSize)
		if _, err := io.ReadFull(conn, payload); err != nil {
			fmt.Fprintln(os.Stderr, "Read payload error:", err)
			return
		}

		// 3) Parse request header from payload
		c := &cursor{b: payload}
		apiKey, apiVer, corrID, clientID, ok := parseHeader(c)
		if !ok {
			// Malformed; close connection
			fmt.Fprintln(os.Stderr, "Malformed header; closing")
			return
		}
		fmt.Println("API Key:", apiKey, "Version:", apiVer, "CorrelationID:", corrID, "ClientID:", clientID)

		// 4) Decide error code for ApiVersions
		errCode := errNone
		if apiKey == apiKeyApiVersions && (apiVer > maxSupportedAPIVer || apiVer < 0) {
			errCode = errUnsupportedVer
		}

		// 5) Build and send flexible ApiVersions response (v3+ body),
		//    but use legacy v0 response header (corrId only) as before.
		resp := buildApiVersionsResponse(corrID, errCode)
		if _, err := conn.Write(resp); err != nil {
			fmt.Fprintln(os.Stderr, "Write error:", err)
			return
		}
		// Loop to read the next request on the same connection.
	}
}

func parseHeader(c *cursor) (apiKey int16, apiVer int16, corrID int32, clientID string, ok bool) {
	var err error
	// Kafka request payload starts with:
	// api_key (int16), api_version (int16), correlation_id (int32), client_id (legacy STRING or compact nullable)
	if apiKey, err = c.i16(); err != nil {
		return
	}
	if apiVer, err = c.i16(); err != nil {
		return
	}
	if corrID, err = c.i32(); err != nil {
		return
	}

	// Try legacy STRING clientId first
	save := c.off
	if clientID, err = c.str16(); err == nil {
		return apiKey, apiVer, corrID, clientID, true
	}
	// Try flexible compact nullable + tagged fields
	c.off = save
	if clientID, err = c.compactNullableString(); err != nil {
		return
	}
	if err = c.skipTagged(); err != nil {
		return
	}
	return apiKey, apiVer, corrID, clientID, true
}

func buildApiVersionsResponse(corrID int32, errCode int16) []byte {
	// Body (flex v3+):
	// error_code (INT16)
	// api_keys (COMPACT_ARRAY) -> 1 element: {api_key=18, min=0, max=4, TAGS=0}
	// throttle_time_ms (INT32) = 0
	// response TAG_BUFFER count = 0
	body := make([]byte, 0, 32)

	// error_code
	body = append(body, byte(errCode>>8), byte(errCode))

	// compact array length = N+1; we advertise one entry => 0x02
	body = append(body, 0x02)

	// element
	body = append(body, 0x00, 0x12) // api_key = 18
	body = append(body, 0x00, 0x00) // min_version = 0
	body = append(body, 0x00, 0x04) // max_version = 4
	body = append(body, 0x00)       // element TAG_BUFFER count = 0

	// throttle_time_ms = 0
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, 0)
	body = append(body, tmp...)

	// response TAG_BUFFER count = 0
	body = append(body, 0x00)

	// Frame: [length][correlationId][body], where length = len(corrId)+len(body)
	resp := make([]byte, 4+4+len(body))
	binary.BigEndian.PutUint32(resp[0:4], uint32(4+len(body)))
	binary.BigEndian.PutUint32(resp[4:8], uint32(corrID))
	copy(resp[8:], body)
	return resp
}
