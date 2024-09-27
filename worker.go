package main

import (
	"fmt"
	"mr/shared"
	"os"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "No Input Files provided ")
		os.Exit(1)
	}

	x := shared.Map("example.txt")
	fmt.Print(x)
}
