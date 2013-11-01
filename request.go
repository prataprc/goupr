package goupr

import (
    "encoding/binary"
    "fmt"
)

// A UPR Request
type Request struct {
    Opcode CommandCode // The command being issued
    Opaque uint32      // An opaque value to be returned with this request
    Cas uint64         // The CAS (if applicable, or 0)
    VBucket uint16     // The vbucket to which this command belongs
    Extras, Key, Body []byte
                       // Command extras, key, and body
}

func NewRequest(opcode CommandCode,
    cas uint64, opaque uint32, vbucket uint16) *Request {
    return &Request{
        Opcode: opcode,
        Cas: cas,
        Opaque: opaque,
        VBucket: vbucket,
    }
}

// The number of bytes this request requires.
func (req *Request) Size() int {
    return HDR_LEN + len(req.Extras) + len(req.Key) + len(req.Body)
}

// A debugging string representation of this request
func (req Request) String() string {
    return fmt.Sprintf("{Request opcode=%s, bodylen=%d, key='%s'}",
        req.Opcode, len(req.Body), req.Key)
}

func (req *Request) fillHeaderBytes(data []byte) int {
    pos := 0
    data[pos] = REQ_MAGIC               // #Magic(0)

    pos++
    data[pos] = byte(req.Opcode)        // #Opcode(1)

    pos++
    binary.BigEndian.PutUint16(data[pos:pos+2], uint16(len(req.Key)))
                                        // #Keylen(2,3)
    pos += 2
    data[pos] = byte(len(req.Extras))   // #Extraslen(4)

    pos++
    data[pos] = 0                       // #Datatype(5)

    pos++
    binary.BigEndian.PutUint16(data[pos:pos+2], req.VBucket)
                                        // #VID(6,7)

    pos += 2
    binary.BigEndian.PutUint32(data[pos:pos+4],
        uint32(len(req.Body)+len(req.Key)+len(req.Extras)))
                                        // #Totallen(8,9,10,11)

    pos += 4
    binary.BigEndian.PutUint32(data[pos:pos+4], req.Opaque)
                                        // #Opaque(12,13,14,15)

    pos += 4
    if req.Cas != 0 {                   // #CAS(16,17,...,23)
        binary.BigEndian.PutUint64(data[pos:pos+8], req.Cas)
    }

    pos += 8
    if len(req.Extras) > 0 {            // #Extras
        copy(data[pos:pos+len(req.Extras)], req.Extras)
        pos += len(req.Extras)
    }

    if len(req.Key) > 0 {               // #Key
        copy(data[pos:pos+len(req.Key)], req.Key)
        pos += len(req.Key)
    }
    return pos
}

// The wire representation of the header (with the extras and key)
func (req *Request) HeaderBytes() []byte {
    data := make([]byte, HDR_LEN+len(req.Extras)+len(req.Key))
    req.fillHeaderBytes(data)
    return data
}

// The wire representation of this request.
func (req *Request) Bytes() []byte {
    data := make([]byte, req.Size())
    pos := req.fillHeaderBytes(data)
    if len(req.Body) > 0 {
        copy(data[pos:pos+len(req.Body)], req.Body)
    }
    return data
}

// Send this request message across a writer.
func (req *Request) Transmit(client *Client) (error) {
    var err error
    if len(req.Body) < 128 {
        _, err = client.conn.Write(req.Bytes())
    } else {
        if _, err = client.conn.Write(req.HeaderBytes()); err == nil {
            _, err = client.conn.Write(req.Body)
        }
    }
    return err
}

// Fill this Request with the data from this reader.
func (req *Request) Receive(client *Client, hdrBytes []byte) error {
    if len(hdrBytes) < HDR_LEN {
        hdrBytes = make([]byte, HDR_LEN)
    }
    _, err := client.conn.Read(hdrBytes)
    if err != nil {
        return err
    }

    if hdrBytes[0] != RES_MAGIC && hdrBytes[0] != REQ_MAGIC {
        return fmt.Errorf("Bad magic: 0x%02x", hdrBytes[0])
    }

    req.Opcode = CommandCode(hdrBytes[1])
    klen := int(binary.BigEndian.Uint16(hdrBytes[2:]))
    elen := int(hdrBytes[4])
    req.VBucket = binary.BigEndian.Uint16(hdrBytes[6:])

    bodyLen := int(binary.BigEndian.Uint32(hdrBytes[8:]) -
        uint32(klen) - uint32(elen))

    req.Opaque = binary.BigEndian.Uint32(hdrBytes[12:])
    req.Cas = binary.BigEndian.Uint64(hdrBytes[16:])

    buf := make([]byte, klen+elen+bodyLen)
    _, err = client.conn.Read(buf)
    if err == nil {
        elen += extendedExtras(req, buf)
        req.Extras = buf[:elen]
        req.Key = buf[elen : klen+elen]
        req.Body = buf[klen+elen:]
    }

    return err
}

func extendedExtras(req *Request, buf []byte) int {
    if req.Opcode >= TAP_MUTATION &&
        req.Opcode <= TAP_CHECKPOINT_END &&
        len(buf) > 1 {
        // In these commands there is "engine private"
        // data at the end of the extras.  The first 2
        // bytes of extra data give its length.
        return int(binary.BigEndian.Uint16(buf))
    }
    return 0
}
