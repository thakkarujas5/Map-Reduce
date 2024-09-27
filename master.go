package main

import (
	"fmt"
	"mr/master"
	"mr/shared"
	"os"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "No Input Files provided ")
		os.Exit(1)
	}

	master.MakeMaster(os.Args[1:], 10)
	x := shared.Map("example.txt")
	fmt.Print(x)
}
