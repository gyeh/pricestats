// mkfixture creates a small representative Parquet fixture from a larger file.
// Two-pass: first scans all rows to find diverse candidates, then selects the best N.
// Usage: go run ./cmd/mkfixture --in testdata/nyu-tisch.parquet --out testdata/nyu-tisch-small.parquet --rows 200
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	goparquet "github.com/parquet-go/parquet-go"

	"github.com/gyeh/pricestats/internal/model"
)

func main() {
	in := flag.String("in", "testdata/nyu-tisch.parquet", "input parquet")
	out := flag.String("out", "testdata/nyu-tisch-small.parquet", "output parquet")
	maxRows := flag.Int("rows", 200, "max rows to output")
	checkOnly := flag.Bool("check", false, "only print stats, don't write")
	flag.Parse()

	f, err := os.Open(*in)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open input: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()
	stat, _ := f.Stat()
	pf, err := goparquet.OpenFile(f, stat.Size())
	if err != nil {
		fmt.Fprintf(os.Stderr, "open parquet: %v\n", err)
		os.Exit(1)
	}

	reader := goparquet.NewGenericReader[model.HospitalChargeRow](pf)
	defer reader.Close()

	if *checkOnly {
		buf := make([]model.HospitalChargeRow, 1024)
		payerCount := 0
		negDollarCount := 0
		total := 0
		for {
			n, readErr := reader.Read(buf)
			for i := 0; i < n; i++ {
				total++
				if buf[i].PayerName != nil && *buf[i].PayerName != "" {
					payerCount++
					if payerCount <= 3 {
						fmt.Printf("Payer row %d: payer=%q plan=%v negDollar=%v\n",
							total, *buf[i].PayerName, buf[i].PlanName, buf[i].NegotiatedDollar)
					}
				}
				if buf[i].NegotiatedDollar != nil {
					negDollarCount++
				}
			}
			if readErr == io.EOF {
				break
			}
		}
		fmt.Printf("\nTotal: %d, Payer: %d, NegDollar: %d\n", total, payerCount, negDollarCount)
		return
	}

	// Pass 1: read ALL rows, bucket by interesting traits.
	type bucket struct {
		name string
		rows []model.HospitalChargeRow
		want int
	}
	buckets := []*bucket{
		{name: "CPT", want: 30},
		{name: "DRG", want: 20},
		{name: "payer", want: 40},
		{name: "neg_dollar", want: 20},
		{name: "general", want: 0},
	}
	bucketMap := make(map[string]*bucket)
	for _, b := range buckets {
		bucketMap[b.name] = b
	}

	buf := make([]model.HospitalChargeRow, 1024)
	var totalRead int
	for {
		n, readErr := reader.Read(buf)
		for i := 0; i < n; i++ {
			totalRead++
			row := buf[i]
			codes := row.CodeValues()

			placed := false
			if ptr := codes["CPT"]; ptr != nil && *ptr != "" && len(bucketMap["CPT"].rows) < bucketMap["CPT"].want {
				bucketMap["CPT"].rows = append(bucketMap["CPT"].rows, row)
				placed = true
			}
			if ptr := codes["DRG"]; ptr != nil && *ptr != "" && len(bucketMap["DRG"].rows) < bucketMap["DRG"].want {
				bucketMap["DRG"].rows = append(bucketMap["DRG"].rows, row)
				placed = true
			}
			if row.PayerName != nil && *row.PayerName != "" && len(bucketMap["payer"].rows) < bucketMap["payer"].want {
				bucketMap["payer"].rows = append(bucketMap["payer"].rows, row)
				placed = true
			}
			if row.NegotiatedDollar != nil && len(bucketMap["neg_dollar"].rows) < bucketMap["neg_dollar"].want {
				bucketMap["neg_dollar"].rows = append(bucketMap["neg_dollar"].rows, row)
				placed = true
			}
			if !placed && len(bucketMap["general"].rows) < *maxRows {
				bucketMap["general"].rows = append(bucketMap["general"].rows, row)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			fmt.Fprintf(os.Stderr, "read: %v\n", readErr)
			os.Exit(1)
		}
	}
	fmt.Printf("Scanned %d rows\n", totalRead)

	// Merge buckets in priority order
	var selected []model.HospitalChargeRow
	for _, b := range buckets {
		if b.name == "general" {
			continue
		}
		for _, row := range b.rows {
			if len(selected) >= *maxRows {
				break
			}
			selected = append(selected, row)
		}
	}
	for _, row := range bucketMap["general"].rows {
		if len(selected) >= *maxRows {
			break
		}
		selected = append(selected, row)
	}

	// Write output
	outFile, err := os.Create(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create output: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	writer := goparquet.NewGenericWriter[model.HospitalChargeRow](outFile)
	if _, err := writer.Write(selected); err != nil {
		fmt.Fprintf(os.Stderr, "write: %v\n", err)
		os.Exit(1)
	}
	if err := writer.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close writer: %v\n", err)
		os.Exit(1)
	}

	// Print summary
	codeCounts := make(map[string]int)
	payerCount := 0
	negDollarCount := 0
	for _, row := range selected {
		for name, ptr := range row.CodeValues() {
			if ptr != nil && *ptr != "" {
				codeCounts[name]++
			}
		}
		if row.PayerName != nil && *row.PayerName != "" {
			payerCount++
		}
		if row.NegotiatedDollar != nil {
			negDollarCount++
		}
	}
	fmt.Printf("Wrote %d rows to %s\n", len(selected), *out)
	fmt.Println("Code distribution:")
	for _, ct := range model.AllCodeTypes {
		if c := codeCounts[ct.Name]; c > 0 {
			fmt.Printf("  %-10s %d\n", ct.Name, c)
		}
	}
	fmt.Printf("  %-10s %d\n", "payer", payerCount)
	fmt.Printf("  %-10s %d\n", "neg_dollar", negDollarCount)
}
