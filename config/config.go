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
		GatewayAddress string `yaml:"gatewayAddress"`
	}

	// API contains the listen address of the API server
	API struct {
		Address  string `yaml:"address"`
		Password string `yaml:"password"`
	}

	// Config contains the configuration for siapfsd
	Config struct {
		Renterd Renterd `yaml:"renterd"`
		IPFS    IPFS    `yaml:"ipfs"`
		API     API     `yaml:"api"`
	}
)