package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/andycostintoma/sql-boundarycheck/internal"
)

func main() {
	configPath := flag.String("config", ".sqlboundarycheck.yaml", "path to config file (relative to root)")
	root := flag.String("root", ".", "project root directory")
	flag.Parse()

	result := internal.Run(*root, *configPath)
	fmt.Print(internal.FormatResult(result))

	if result.HasViolations() || result.HasErrors() {
		os.Exit(1)
	}
}
