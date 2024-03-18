package main

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/lightninglabs/loop/looprpc"
	"github.com/urfave/cli"
)

var assetsCommands = cli.Command{

	Name:      "assets",
	ShortName: "a",
	Usage:     "manage asset swaps",
	Description: `
	`,
	Subcommands: []cli.Command{
		assetsOutCommand,
		listOutCommand,
		// buyOutCommand,
	},
}
var (
	assetsOutCommand = cli.Command{
		Name:      "out",
		ShortName: "o",
		Usage:     "swap asset out",
		ArgsUsage: "",
		Description: `
		List all reservations.
	`,
		Flags: []cli.Flag{
			cli.Uint64Flag{
				Name:  "amt",
				Usage: "the amount in satoshis to loop out.",
			},
			cli.StringFlag{
				Name:  "asset_id",
				Usage: "asset_id",
			},
		},
		Action: assetSwapOut,
	}
	// buyOutCommand = cli.Command{
	// 	Name:      "buy",
	// 	ShortName: "b",
	// 	Usage:     "buy asset output",
	// 	ArgsUsage: "",
	// 	Description: `
	// 	List all reservations.
	// `,
	// 	Flags: []cli.Flag{
	// 		cli.StringFlag{
	// 			Name:  "swap_hash",
	// 			Usage: "swap hash",
	// 		},
	// 	},
	// 	Action: buyOut,
	// }
	listOutCommand = cli.Command{
		Name:      "list",
		ShortName: "l",
		Usage:     "list asset swaps",
		ArgsUsage: "",
		Description: `
		List all reservations.
	`,
		Action: listOut,
	}
)

func assetSwapOut(ctx *cli.Context) error {
	// First set up the swap client itself.
	client, cleanup, err := getAssetsClient(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	args := ctx.Args()

	var amtStr string
	switch {
	case ctx.IsSet("amt"):
		amtStr = ctx.String("amt")
	case ctx.NArg() > 0:
		amtStr = args[0]
		args = args.Tail()
	default:
		// Show command help if no arguments and flags were provided.
		return cli.ShowCommandHelp(ctx, "out")
	}

	amt, err := parseAmt(amtStr)
	if err != nil {
		return err
	}

	assetId, err := hex.DecodeString(ctx.String("asset_id"))
	if err != nil {
		return err
	}

	if len(assetId) != 32 {
		return fmt.Errorf("invalid asset id")
	}

	res, err := client.SwapOut(
		context.Background(),
		&looprpc.SwapOutRequest{
			Amt:   uint64(amt),
			Asset: assetId,
		},
	)
	if err != nil {
		return err
	}

	printRespJSON(res)
	return nil
}

// func buyOut(ctx *cli.Context) error {
// 	// First set up the swap client itself.
// 	client, cleanup, err := getAssetsClient(ctx)
// 	if err != nil {
// 		return err
// 	}
// 	defer cleanup()

// 	swapHash, err := hex.DecodeString(ctx.String("swap_hash"))
// 	if err != nil {
// 		return err
// 	}

// 	if len(swapHash) != 32 {
// 		return fmt.Errorf("invalid asset id")
// 	}

// 	res, err := client.BuyOutput(
// 		context.Background(),
// 		&looprpc.BuyOutputRequest{
// 			SwapHash: swapHash,
// 		},
// 	)
// 	if err != nil {
// 		return err
// 	}

//		printRespJSON(res)
//		return nil
//	}
func listOut(ctx *cli.Context) error {
	// First set up the swap client itself.
	client, cleanup, err := getAssetsClient(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	res, err := client.ListAssetSwaps(
		context.Background(),
		&looprpc.ListAssetSwapsRequest{},
	)
	if err != nil {
		return err
	}

	printRespJSON(res)
	return nil
}

func getAssetsClient(ctx *cli.Context) (looprpc.AssetsClientClient, func(), error) {
	rpcServer := ctx.GlobalString("rpcserver")
	tlsCertPath, macaroonPath, err := extractPathArgs(ctx)
	if err != nil {
		return nil, nil, err
	}
	conn, err := getClientConn(rpcServer, tlsCertPath, macaroonPath)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { conn.Close() }

	loopClient := looprpc.NewAssetsClientClient(conn)
	return loopClient, cleanup, nil
}
