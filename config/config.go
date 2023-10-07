package config

type (
	// Renterd contains the address, password, and bucket on the renterd worker
	Renterd struct {
		Address  string `yaml:"address"`
		Password string `yaml:"password"`
		Bucket   string `yaml:"bucket"`
	}

	// IPFS contains the listen address of the IPFS gateway
	IPFS struct {
		PrivateKey        string   `yaml:"privateKey"`
		GatewayAddress    string   `yaml:"gatewayAddress"`
		ListenAddresses   []string `yaml:"listenAddresses"`
		AnnounceAddresses []string `yaml:"announceAddresses"`
	}

	// API contains the listen address of the API server
	API struct {
		Address  string `yaml:"address"`
		Password string `yaml:"password"`
	}

	// Config contains the configuration for fsd
	Config struct {
		Renterd Renterd `yaml:"renterd"`
		IPFS    IPFS    `yaml:"ipfs"`
		API     API     `yaml:"api"`
	}
)
