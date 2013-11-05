// Memcached binary protocol packet formats and constants.
package goupr

import (
    "github.com/dustin/gomemcached"
)

const (
    // Opcodes for UPR
    UPR_OPEN             = gomemcached.CommandCode(0x50)
    UPR_ADD_STREAM       = gomemcached.CommandCode(0x51)
    UPR_CLOSE_STREAM     = gomemcached.CommandCode(0x52)
    UPR_FAILOVER_LOG     = gomemcached.CommandCode(0x54)
    UPR_STREAM_REQ       = gomemcached.CommandCode(0x53)
    UPR_STREAM_END       = gomemcached.CommandCode(0x55)
    UPR_SNAPSHOTM        = gomemcached.CommandCode(0x56)
    UPR_MUTATION         = gomemcached.CommandCode(0x57)
    UPR_DELETION         = gomemcached.CommandCode(0x58)
    UPR_EXPIRATION       = gomemcached.CommandCode(0x59)
    UPR_FLUSH            = gomemcached.CommandCode(0x5a)
)

const (
    // UPR Status
    ROLLBACK        = gomemcached.Status(0x23)
)

func init() {
    gomemcached.CommandNames[UPR_OPEN] = "UPR_OPEN"
    gomemcached.CommandNames[UPR_ADD_STREAM] = "UPR_ADD_STREAM"
    gomemcached.CommandNames[UPR_CLOSE_STREAM] = "UPR_CLOSE_STREAM"
    gomemcached.CommandNames[UPR_FAILOVER_LOG] = "UPR_FAILOVER_LOG"
    gomemcached.CommandNames[UPR_STREAM_REQ] = "UPR_STREAM_REQ"
    gomemcached.CommandNames[UPR_STREAM_END] = "UPR_STREAM_END"
    gomemcached.CommandNames[UPR_SNAPSHOTM] = "UPR_SNAPSHOTM"
    gomemcached.CommandNames[UPR_MUTATION] = "UPR_MUTATION"
    gomemcached.CommandNames[UPR_DELETION] = "UPR_DELETION"
    gomemcached.CommandNames[UPR_EXPIRATION] = "UPR_EXPIRATION"
    gomemcached.CommandNames[UPR_FLUSH] = "UPR_FLUSH"
}
