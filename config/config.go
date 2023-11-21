package config

type (
	// Renterd contains the address, password, and bucket on the renterd worker
	Renterd struct {
		Address  string `yaml:"address"`
		Password string `yaml:"password"`
		Bucket   string `yaml:"bucket"`
	}

	// HTTPGateway contains the configuration for the IPFS HTTP gateway
	HTTPGateway struct {
		ListenAddress     string `yaml:"ListenAddress"`
		RedirectPathStyle bool   `yaml:"redirectPathStyle"`
	}

	// IPFS contains the configuration for the IPFS node
	IPFS struct {
		PrivateKey        string      `yaml:"privateKey"`
		ListenAddresses   []string    `yaml:"listenAddresses"`
		AnnounceAddresses []string    `yaml:"announceAddresses"`
		FetchRemote       bool        `yaml:"fetchRemote"`
		Gateway           HTTPGateway `yaml:"gateway"`
	}

	// API contains the listen address of the API server
	API struct {
		Address  string `yaml:"address"`
		Password string `yaml:"password"`
	}

	// Log contains the log settings
	Log struct {
		Level string `yaml:"level"`
	}

	// Config contains the configuration for fsd
	Config struct {
		Renterd Renterd `yaml:"renterd"`
		IPFS    IPFS    `yaml:"ipfs"`
		API     API     `yaml:"api"`
		Log     Log     `yaml:"log"`
	}
)
