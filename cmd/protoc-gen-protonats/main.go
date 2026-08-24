package main

import (
	"google.golang.org/protobuf/compiler/protogen"

	"github.com/mudomi/protonats/internal/gen"
)

func main() {
	protogen.Options{}.Run(func(plugin *protogen.Plugin) error {
		for _, file := range plugin.Files {
			if file.Generate {
				if err := gen.GenerateFile(plugin, file); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
