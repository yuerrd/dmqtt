package codec

// MQTT 5.0 CONNACK Reason Codes (§3.2.2.2)
const (
	ReasonSuccess                byte = 0x00
	ReasonUnspecifiedError       byte = 0x80
	ReasonMalformedPacket        byte = 0x81
	ReasonProtocolError          byte = 0x82
	ReasonImplSpecificError      byte = 0x83
	ReasonUnsupportedProtocol    byte = 0x84
	ReasonClientIDNotValid       byte = 0x85
	ReasonBadUserNameOrPassword  byte = 0x86
	ReasonNotAuthorized          byte = 0x87
	ReasonServerUnavailable      byte = 0x88
	ReasonServerBusy             byte = 0x89
	ReasonBanned                 byte = 0x8A
	ReasonBadAuthMethod          byte = 0x8C
	ReasonTopicNameInvalid       byte = 0x90
	ReasonPacketTooLarge         byte = 0x95
	ReasonQuotaExceeded          byte = 0x97
	ReasonPayloadFormatInvalid   byte = 0x99
	ReasonRetainNotSupported     byte = 0x9A
	ReasonQoSNotSupported        byte = 0x9B
	ReasonUseAnotherServer       byte = 0x9C
	ReasonServerMoved            byte = 0x9D
	ReasonConnectionRateExceeded byte = 0x9F
)

// MQTT 5.0 Generic ACK Reason Codes (§3.4.2.1, etc.)
const (
	ReasonCodeSuccess          byte = 0x00
	ReasonCodeNoMatchingSub    byte = 0x10
	ReasonCodeUnspecifiedError byte = 0x80
	ReasonCodeNotAuthorized    byte = 0x87
	ReasonCodeTopicNameInvalid byte = 0x90
	ReasonCodePacketIDInUse    byte = 0x91
	ReasonCodeQuotaExceeded    byte = 0x97
)

// MQTT 5.0 DISCONNECT Reason Codes (§3.14.2.1)
const (
	DisconnNormalDisconnection     byte = 0x00
	DisconnWithWillMessage         byte = 0x04
	DisconnUnspecifiedError        byte = 0x80
	DisconnMalformedPacket         byte = 0x81
	DisconnProtocolError           byte = 0x82
	DisconnImplSpecificError       byte = 0x83
	DisconnNotAuthorized           byte = 0x87
	DisconnServerBusy              byte = 0x89
	DisconnServerShuttingDown      byte = 0x8B
	DisconnKeepAliveTimeout        byte = 0x8D
	DisconnSessionTakenOver        byte = 0x8E
	DisconnTopicFilterInvalid      byte = 0x8F
	DisconnTopicNameInvalid        byte = 0x90
	DisconnReceiveMaxExceeded      byte = 0x93
	DisconnTopicAliasInvalid       byte = 0x94
	DisconnPacketTooLarge          byte = 0x95
	DisconnMessageRateExceeded     byte = 0x96
	DisconnQuotaExceeded           byte = 0x97
	DisconnAdminAction             byte = 0x98
	DisconnPayloadFormatInvalid    byte = 0x99
	DisconnRetainNotSupported      byte = 0x9A
	DisconnQoSNotSupported         byte = 0x9B
	DisconnUseAnotherServer        byte = 0x9C
	DisconnServerMoved             byte = 0x9D
	DisconnSharedSubNotSupported   byte = 0x9E
	DisconnConnectionRateExceeded  byte = 0x9F
	DisconnMaxConnectTime          byte = 0xA0
	DisconnSubIDNotSupported       byte = 0xA1
	DisconnWildcardSubNotSupported byte = 0xA2
)
