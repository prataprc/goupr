package goupr

import (
    "encoding/binary"
    "fmt"
)

// A UPR Response
type Response struct {
    Opcode CommandCode // The command opcode that sent the request
    Opaque uint32      // The opaque sent in the request
    Cas uint64         // The CAS identifier (if applicable)
    Status Status      // The status of the response
    Extras, Key, Body []byte
                // Extras, key, and body for this response
}

// A debugging string representation of this response
func (res Response) String() string {
    return fmt.Sprintf("{Response status=%v keylen=%d, extralen=%d, bodylen=%d}",
        res.Status, len(res.Key), len(res.Extras), len(res.Body))
}

// Response as an error.
func (res Response) Error() string {
    return fmt.Sprintf("Response status=%v, opcode=%v, opaque=%v, msg: %s",
        res.Status, res.Opcode, res.Opaque, string(res.Body))
}

func errStatus(e error) Status {
    status := Status(0xffff)
    if res, ok := e.(Response); ok {
        status = res.Status
    }
    if res, ok := e.(*Response); ok {
        status = res.Status
    }
    return status
}

// Number of bytes this response consumes on the wire.
func (res *Response) Size() int {
    return HDR_LEN + len(res.Extras) + len(res.Key) + len(res.Body)
}

func (res *Response) fillHeaderBytes(data []byte) int {
    pos := 0
    data[pos] = RES_MAGIC               // #Magic(0)

    pos++
    data[pos] = byte(res.Opcode)        // #Opcode(1)

    pos++
    binary.BigEndian.PutUint16(data[pos:pos+2], uint16(len(res.Key)))
                                        // #Keylen(2,3)

    pos += 2
    data[pos] = byte(len(res.Extras))   // #Extralen(4)

    pos++
    data[pos] = 0                       // #Datatype(5)

    pos++
    binary.BigEndian.PutUint16(data[pos:pos+2], uint16(res.Status))
                                        // #Status(6,7)

    pos += 2
    binary.BigEndian.PutUint32(data[pos:pos+4],
        uint32(len(res.Body)+len(res.Key)+len(res.Extras)))
                                        // #Totallen(8,9,10,11)

    pos += 4
    binary.BigEndian.PutUint32(data[pos:pos+4], res.Opaque)
                                        // #Opaque(12,13,14,15)

    pos += 4
    binary.BigEndian.PutUint64(data[pos:pos+8], res.Cas)
                                        // #CAS(16,17,...,23)

    pos += 8
    if len(res.Extras) > 0 {            // #Extras
        copy(data[pos:pos+len(res.Extras)], res.Extras)
        pos += len(res.Extras)
    }

    if len(res.Key) > 0 {               // #Keys
        copy(data[pos:pos+len(res.Key)], res.Key)
        pos += len(res.Key)
    }
    return pos
}

// Get just the header bytes for this response.
func (res *Response) HeaderBytes() []byte {
    data := make([]byte, HDR_LEN+len(res.Extras)+len(res.Key))
    res.fillHeaderBytes(data)
    return data
}

// The actual bytes transmitted for this response.
func (res *Response) Bytes() []byte {
    data := make([]byte, res.Size())
    pos := res.fillHeaderBytes(data)
    copy(data[pos:pos+len(res.Body)], res.Body)
    return data
}

// Send this response message across a writer.
func (res *Response) Transmit(client *Client) error {
    var err error
    if len(res.Body) < 128 {
        if _, err = client.conn.Write(res.Bytes()); err != nil {
            client.tryError(err)
            return err
        }
    } else {
        if _, err = client.conn.Write(res.HeaderBytes()); err != nil {
            client.tryError(err)
            return err
        } else if _, err = client.conn.Write(res.Body); err != nil {
            client.tryError(err)
            return err
        }
    }
    return nil
}

// Fill this Response with the data from this reader.
func (res *Response) Receive(client *Client, hdrBytes []byte) error {
    if len(hdrBytes) < HDR_LEN {
        hdrBytes = make([]byte, 24)
    }

    if _, err := client.conn.Read(hdrBytes); err != nil {
        client.tryError(err)
        return err
    }

    if hdrBytes[0] != RES_MAGIC && hdrBytes[0] != REQ_MAGIC {
        return fmt.Errorf("Bad magic: 0x%02x", hdrBytes[0])
    }

    klen := int(binary.BigEndian.Uint16(hdrBytes[2:4]))
    elen := int(hdrBytes[4])

    res.Opcode = CommandCode(hdrBytes[1])
    res.Status = Status(binary.BigEndian.Uint16(hdrBytes[6:8]))
    res.Opaque = binary.BigEndian.Uint32(hdrBytes[12:16])
    res.Cas = binary.BigEndian.Uint64(hdrBytes[16:24])

    bodyLen := int(binary.BigEndian.Uint32(hdrBytes[8:12])) - (klen + elen)

    buf := make([]byte, klen+elen+bodyLen)
    if _, err := client.conn.Read(buf); err != nil {
        client.tryError(err)
        return err
    }
    res.Extras = buf[0:elen]
    res.Key = buf[elen : klen+elen]
    res.Body = buf[klen+elen:]
    return nil
}
