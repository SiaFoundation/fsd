package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"

	iface "github.com/ipfs/boxo/coreiface"
	"github.com/ipfs/boxo/coreiface/options"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
)

func uploadHandler(api iface.CoreAPI) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		err := r.ParseMultipartForm(10 << 20) // Limit file size
		if err != nil {
			http.Error(w, "Couldn't parse multipart form", http.StatusBadRequest)
			return
		}

		fileHeader := r.MultipartForm.File["file"]
		if len(fileHeader) == 0 {
			http.Error(w, "No file provided", http.StatusBadRequest)
			return
		}

		file, err := fileHeader[0].Open()
		if err != nil {
			http.Error(w, "Couldn't open file", http.StatusInternalServerError)
			return
		}
		defer file.Close()

		data, err := io.ReadAll(file)
		if err != nil {
			http.Error(w, "Unable to read the file", http.StatusBadRequest)
			return
		}

		// TODO: optionally push to an IPFS node for actual p2p sharing

		ctx := context.Background()
		cidFile, err := api.Unixfs().Add(ctx, files.NewBytesFile(data), options.Unixfs.Pin(false))
		if err != nil {
			http.Error(w, "Failed to add file", http.StatusInternalServerError)
			return
		}

		// TODO: save to configurable renterd instance
		if err := saveFileToDisk(data, cidFile.Cid().String()); err != nil {
			http.Error(w, "Failed to save file", http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "CID: %s\n", cidFile.Cid())
	}
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	cid := filepath.Base(r.URL.Path)
	data, err := os.ReadFile(filepath.Join("filestore", cid))
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	w.Write(data)
}

func ensureFileStoreExists() {
	if _, err := os.Stat("filestore"); os.IsNotExist(err) {
		os.Mkdir("filestore", 0755)
	}
}

func saveFileToDisk(data []byte, cid string) error {
	filePath := filepath.Join("filestore", cid)
	return os.WriteFile(filePath, data, 0644)
}

func main() {
	ctx := context.Background()
	ensureFileStoreExists()
	node, err := core.NewNode(ctx, &core.BuildCfg{
		Online: false,
	})
	if err != nil {
		fmt.Println("Failed to start IPFS node:", err)
		return
	}
	defer node.Close()

	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		fmt.Println("Failed to create core API")
		return
	}

	http.HandleFunc("/upload", uploadHandler(api))
	http.HandleFunc("/ipfs/", downloadHandler)

	server := &http.Server{Addr: ":8080"}

	go func() {
		fmt.Println("Server running on :8080")
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Printf("ListenAndServe(): %s\n", err)
		}
	}()

	// Graceful shutdown on Ctrl+C
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop
	fmt.Println("\nShutting down server...")
	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf("Error shutting down: %s\n", err)
	}
}
