package main

import (
	"fmt"
	"mr/master"
	"os"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "No Input Files provided ")
		os.Exit(1)
	}

	master.MakeMaster(os.Args[1:], 10)
}
