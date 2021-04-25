package main

import (
	"fmt"
	"math"
	"os"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/skratchdot/open-golang/open"
	"github.com/sqweek/dialog"
)

func svCleaner(s series.Series) series.Series {
	//convert series to float, if one of H10's stupid nil values set a small value in its place
	//SV needs to be non-zero for log transformation in future steps
	f := s.Float()
	if math.IsNaN(f[0]) {
		return series.Floats(.0001)

	}
	return series.Floats(f[0])
}

func rankCleaner(s series.Series) series.Series {
	//same deal, just with ints
	num, err := s.Int()
	if err != nil {
		return series.Ints(1000)
	}
	return series.Ints(num[0])

}

func relevanceCounter(s series.Series) series.Series {
	//iterate through columns adding asins ranked below 60
	//with organic rank volatility, threshold of 60 is more likely to catch asins having a bad day
	//in the end, 60 is an arbitrary choice
	num, err := s.Int()
	if err != nil {
		panic(err)
	}
	sum := 0
	for i := 0; i < len(num); i++ {
		if num[i] < 60 {
			sum += 1
		}
	}

	return series.Ints(sum)

}

//TODO make this calculation use variable weights
func organicValue(s series.Series) series.Series {
	//convert columns to float
	conv := s.Float()
	//assumes search volume is first column and relevance is second
	logVolume := math.Log(conv[0])
	logVolume = math.Round(logVolume*100) / 100
	ov := conv[1] * logVolume
	return series.Floats(ov)

}

func main() {
	//"wHy DoN'T yOu UsE A dIfFeReNt lAnGuAgE" BECAUSE THIS WHOLE PROGRAM IS 30 LINES OF CODE IN PYTHON!!!
	//"bUt PyThOn Is sLoW" the time it took to learn Go syntax, code, and test this will require something like 6000 end-user launches to break even on time.
	//should I have used Java? probably, but Go has neat API stuff and felt like a better personal growth project.
	//will this app be lightyears better than the python utility? almost certainly, but at what cost? My sanity?
	//Pickles.

	//alright, real comments start here
	//workflow: Open CSV > Convert to dataframe > remove junk columns > clean remaining data in slices > perform column mutations > reassemble final df

	//for real didn't know print would show up in the executable window...probably shouldn't have had dumb prints in final code...
	fmt.Println("Welcome to the new and improved, super duper fast keyword cleaner.")
	fmt.Println("Select the Cerebro Export you want de-garbaged")
	fmt.Println("New file will be placed in the same folder as the source.")
	fmt.Println("(At least...I think it will.  If not, check the folder the .exe is in.\nIf it's not there...well...this is a new language for me and I don't really know what I'm doing'.  \nYou'll find it. I believe in you.)")
	fmt.Println("Enjoy your stay.")
	//create open file dialog filtered to .csv files for convenience
	filename, err := dialog.File().Filter("Microsoft Excel Comma Separated Values", "csv").Load()
	if err != nil {
		fmt.Println("Error: ", err)
	}

	//get csv from path
	csvFile, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error :: ", err)
	}
	//create Gota dataframe
	df := dataframe.ReadCSV(csvFile)
	csvFile.Close()
	//drop extra nonsense  THIS IS WHERE NEW CEREBRO COLUMNS NEED TO BE ADDED IF THE EXPORT CHANGES
	df = df.Drop(
		[]string{"Search Volume Trend (30 days)", "Cerebro IQ Score", "Competing Products", "CPR 8-Day Giveaways", "Sponsored ASINs", "Amazon Recommended", "Sponsored", "Organic", "Sponsored Rank (avg)", "Sponsored Rank (count)", "Amazon Recommended Rank (avg)", "Amazon Recommended Rank (count)", "Relative Rank", "Competitor Rank (avg)", "Ranking Competitors (count)", "Competitor Performance Score"},
	)
	//break off keyword and search volume, unfuckify the stupid way H10 handles nulls (turn NaNs into .0001 to allow for log transform)
	phrase := df.Select("Phrase")
	cleanVolume := df.Select("Search Volume").Rapply(svCleaner)
	cleanVolume.SetNames("Estimated Search Volume")

	//initialize final df
	out := dataframe.New(phrase.Col("Phrase"))
	out = out.CBind(cleanVolume)

	//relevance calculation
	//get rid of newly duplicated columns
	df = df.Drop(
		[]string{"Search Volume", "Phrase"},
	)

	//iterate through the remaining columns clean data and replace original column(column names change file to file)
	colNames := df.Names()
	for i := 0; i < len(colNames); i++ {
		cleaned := df.Select(colNames[i]).Rapply(rankCleaner)
		cleaned.SetNames(colNames[i])
		df = df.Mutate(cleaned.Col(colNames[i]))

	}
	//calculate relevance and append to out (holy hell that worked first try. I'm obviously a Go master now.)
	rel := df.Rapply(relevanceCounter)
	rel.SetNames("Relevance")
	out = out.CBind(rel)

	//calculate organic value as (log(volume)*relevance)
	ov := out.Select([]string{"Estimated Search Volume", "Relevance"}).Rapply(organicValue)
	ov.SetNames("Organic Value")
	out = out.CBind(ov)

	//sort by organic value
	out = out.Arrange(
		dataframe.RevSort("Organic Value"),
	)

	//write to disk
	fileName := fmt.Sprintf("./%v.csv", out.Col("Phrase").Val(0))
	f, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	out.WriteCSV(f)

	//launch final file in excel (or default csv reader if you're a weirdo)
	open.Start(fmt.Sprintf("%v.csv", out.Col("Phrase").Val(0)))
}
