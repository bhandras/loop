package main

import (
	"context"
	"fmt"

	"github.com/lightninglabs/loop/looprpc"
	"github.com/urfave/cli"
)

func init() {
	commands = append(commands, assetDepositsCommands)
}

var assetDepositsCommands = cli.Command{
	Name:      "asset deposits",
	ShortName: "ad",
	Usage:     "TAP asset deposit commands.",
	Subcommands: []cli.Command{
		newAssetDepositCommand,
		listAssetDepositsCommand,
		testCoSignCommand,
	},
}

var newAssetDepositCommand = cli.Command{
	Name:      "newdeposit",
	ShortName: "n",
	Usage:     "Create a new asset deposit.",
	Description: `
	TODO: Add description.
	`,
	Action: newAssetDeposit,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "asset_id",
			Usage: "The asset id of the asset to deposit.",
		},
		cli.Uint64Flag{
			Name:  "amt",
			Usage: "the amount to deposit (in asset units).",
		},
		cli.UintFlag{
			Name:  "expiry",
			Usage: "the deposit expiry in blocks.",
		},
	},
}

var testCoSignCommand = cli.Command{
	Name:      "testcosign",
	ShortName: "tcs",
	Usage:     "Test co-signing a deposit to spend to an HTLC.",
	Action:    testCoSign,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "deposit_id",
			Usage: "The deposit id of the asset deposit.",
		},
	},
}

func testCoSign(ctx *cli.Context) error {
	ctxb := context.Background()
	if ctx.NArg() > 0 {
		return cli.ShowCommandHelp(ctx, "testcosign")
	}

	client, cleanup, err := getClient(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	depositID := ctx.String("deposit_id")

	resp, err := client.CoSignAssetDepositHTLC(
		ctxb, &looprpc.CoSignAssetDepositHTLCRequest{
			DepositId: depositID,
		},
	)
	if err != nil {
		return err
	}

	printRespJSON(resp)

	return nil
}

func newAssetDeposit(ctx *cli.Context) error {
	ctxb := context.Background()
	if ctx.NArg() > 0 {
		return cli.ShowCommandHelp(ctx, "newdeposit")
	}

	client, cleanup, err := getClient(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	assetID := ctx.String("asset_id")
	amt := ctx.Uint64("amt")
	expiry := int32(ctx.Uint("expiry"))

	resp, err := client.NewAssetDeposit(
		ctxb, &looprpc.NewAssetDepositRequest{
			AssetId:   assetID,
			Amount:    amt,
			CsvExpiry: expiry,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Joint asset deposit (deposit_id=%v) created.",
		resp.DepositId)

	return nil
}

var listAssetDepositsCommand = cli.Command{
	Name:      "listdeposits",
	ShortName: "l",
	Usage:     "List TAP asset deposits.",
	Description: `
	List all TAP asset deposits.
	`,
	Flags: []cli.Flag{
		cli.UintFlag{
			Name: "min_confs",
			Usage: "The minimum amount of confirmations an " +
				"output should have to be listed.",
		},
		cli.UintFlag{
			Name: "max_confs",
			Usage: "The maximum number of confirmations an " +
				"output could have to be listed.",
		},
	},
	Action: listAssetDeposits,
}

func listAssetDeposits(ctx *cli.Context) error {
	ctxb := context.Background()
	if ctx.NArg() > 0 {
		return cli.ShowCommandHelp(ctx, "listunspent")
	}

	client, cleanup, err := getClient(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	resp, err := client.ListAssetDeposits(
		ctxb, &looprpc.ListAssetDepositsRequest{
			MinConfs: uint32(ctx.Int("min_confs")),
			MaxConfs: uint32(ctx.Int("max_confs")),
		})
	if err != nil {
		return err
	}

	printRespJSON(resp)

	return nil
}
