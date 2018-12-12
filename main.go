package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/breathbath/go_utils/utils/errs"
	"github.com/breathbath/go_utils/utils/fs"
	"github.com/djimenez/iconv-go"
	"github.com/olekukonko/tablewriter"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type Row []string

type Table struct {
	name             string
	counter          int64
	rows             []Row
	columns          []string
	uniqueColIndices []int
	uniqueValues     map[string]int64
}

func NewTable(name string, columns []string) *Table {
	return &Table{
		name:             name,
		counter:          0,
		columns:          columns,
		rows:             []Row{},
		uniqueColIndices: []int{},
		uniqueValues:     make(map[string]int64),
	}
}

func (t *Table) SetUniqueCols(uniqueCols ...string) {
	for _, uniqueCol := range uniqueCols {
		found := false
		for kCol, col := range t.columns {
			if col == uniqueCol {
				found = true
				t.uniqueColIndices = append(t.uniqueColIndices, kCol)
				break
			}
		}
		if !found {
			log.Panicf("Unknown unique column %s", uniqueCol)
		}
	}
}

func (t *Table) AddRow(row ...interface{}) int64 {
	counter := t.counter + 1

	rowColsCount := len(row)
	tableRowsCount := len(t.columns)
	if rowColsCount != tableRowsCount {
		log.Panicf("Columns count %d in a row does not correspond to the table columns count %d", rowColsCount, tableRowsCount)
	}

	concatUniqueValue := ""
	if len(t.uniqueColIndices) > 0 {
		for _, uniqueColIndex := range t.uniqueColIndices {
			concatUniqueValue += fmt.Sprint(row[uniqueColIndex])
		}
		if rowId, ok := t.uniqueValues[concatUniqueValue]; ok {
			return rowId
		}
		t.uniqueValues[concatUniqueValue] = counter
	}

	rowStrs := []string{}
	for _, rowVal := range row {
		rowStrs = append(rowStrs, fmt.Sprint(rowVal))
	}

	t.rows = append(t.rows, rowStrs)
	t.counter = counter

	return counter
}

func (t *Table) Change(val string, col, row int64) {
	if int(row) > len(t.rows)-1 {
		log.Panicf("Row %d is out of range", row)
	}

	if int(col) > len(t.columns)-1 {
		log.Panicf("Column %d is out of range", col)
	}

	t.rows[row][col] = val
}

func (t *Table) String() string {
	buf := new(bytes.Buffer)
	outputTable := tablewriter.NewWriter(buf)
	columns := append([]string{"Id"}, t.columns...)
	outputTable.SetCaption(true, t.name)
	outputTable.SetHeader(columns)
	for k, row := range t.rows {
		row := append([]string{fmt.Sprint(k + 1)}, row...)
		outputTable.Append(row)
	}

	outputTable.Render()

	return buf.String()
}

func main() {
	tables := []*Table{}
	wordsTable := NewTable("words", []string{"word", "dic_id", "lang_id"})

	dictionariesTable := NewTable("dictionaries", []string{"name"})

	translationsTable := NewTable("translations", []string{"word_from_id", "word_to_id"})
	translationsTable.SetUniqueCols("word_from_id", "word_to_id")

	gramTypeTable := NewTable("gramTypes", []string{"value"})
	gramTypeTable.SetUniqueCols("value")

	gramTypeTranslationTable := NewTable("gramTypesToTranslations", []string{"gram_type_id", "translation_id"})
	gramTypeTranslationTable.SetUniqueCols("gram_type_id", "translation_id")

	translationAttributesTable := NewTable("translationAttributes", []string{"value"})
	translationAttributesTable.SetUniqueCols("value")

	translationAttributesToTranslationsTable := NewTable("translationAttributesTranslations", []string{"attribute_id", "translation_id"})
	translationAttributesToTranslationsTable.SetUniqueCols("attribute_id", "translation_id")

	languagesTable := NewTable("languages", []string{"name"})
	languagesTable.SetUniqueCols("name")

	tables = append(
		tables,
		wordsTable,
		dictionariesTable,
		languagesTable,
		translationsTable,
		gramTypeTable,
		gramTypeTranslationTable,
		translationAttributesTable,
	)

	path := flag.String("path", "", "/tmp/my.dsl")
	flag.Parse()
	if *path == "" {
		log.Panic("Path is not provided")
	}

	if !fs.FileExists(*path) {
		log.Panicf("Non existing file path is provided: %s", *path)
	}

	file, err := os.Open(*path)
	errs.FailOnError(err)

	defer file.Close()

	reader, err := iconv.NewReader(file, "utf-16", "utf-8")
	errs.FailOnError(err)

	scanner := bufio.NewScanner(reader)
	var dicId, langFromId, langToId, wordFromId, gramTypeId, attributeId, translationId, wordToId int64
	var dicName, langFromName, langToName string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		if dicName == "" && scanTitle("#NAME", line, &dicName) {
			dicId = dictionariesTable.AddRow(dicName)
			continue
		}

		if langFromName == "" && scanTitle("#INDEX_LANGUAGE", line, &langFromName) {
			langFromId = languagesTable.AddRow(langFromName)
			continue
		}

		if langToName == "" && scanTitle("#CONTENTS_LANGUAGE", line, &langToName) {
			langToId = languagesTable.AddRow(langToName)
			continue
		}

		wordArticleTitle := scanWordArticleTitle(line)
		if wordArticleTitle != "" {
			wordFromId = wordsTable.AddRow(wordArticleTitle, dicId, langFromId)
			gramTypeId, attributeId = 0, 0
			continue
		}
		err := validateBodyLine(line)
		errs.FailOnError(err)

		_, note := extractNote(line)
		translation := extractTranslation(line)
		if note != "" && translation == "" {
			gramTypeId = gramTypeTable.AddRow(note)
			continue
		}

		if translation != "" {
			wordToId = wordsTable.AddRow(translation, dicId, langToId)
			translationId = translationsTable.AddRow(wordFromId, wordToId)
			if note != ""{
				attributeId = translationAttributesTable.AddRow(note)
				translationAttributesToTranslationsTable.AddRow(attributeId, translationId)
				attributeId = 0
			}
			if gramTypeId > 0 {
				gramTypeTranslationTable.AddRow(gramTypeId, translationId)
			}
			continue
		}
	}
	for _, tbl := range tables {
		fmt.Println(tbl)
	}
}

func validateBodyLine(inputLine string) error {
	pattern := regexp.MustCompile(`^\s+`)
	if !pattern.MatchString(inputLine) {
		return fmt.Errorf("Line is not beginning with spaces")
	}

	expectedTags := []string{"[p]", "[trn]", "[*]"}
	for _, expectedTag := range expectedTags {
		if strings.Contains(inputLine, expectedTag) {
			return nil
		}
	}
	return fmt.Errorf(
		"Line '%s' is not containing one of expected tags: %s",
		inputLine,
		strings.Join(expectedTags, ","),
	)
}

func extractIndex(inputLine string) int64 {
	pattern := regexp.MustCompile(`^\s*\[.*?](\d*)[)|.]`)
	res := pattern.FindStringSubmatch(inputLine)
	if res != nil {
		inputLineInt, err := strconv.ParseInt(res[1], 10, 64)
		if err != nil {
			return 0
		}
		return inputLineInt
	}

	return 0
}
func extractTranslation(inputLine string) string {
	pattern := regexp.MustCompile(`\[trn](.*)\[/trn]`)
	res := pattern.FindStringSubmatch(inputLine)
	if res != nil {
		return cleanupLine(res[1])
	}

	return ""
}

func extractTranslationAttributes(inputLine string) (color string, isItalic bool) {
	pattern := regexp.MustCompile(`.*\[/.*?](.*)\[trn]`)
	res := pattern.FindStringSubmatch(inputLine)
	if res == nil {
		return "", false
	}

	isItalic = strings.Contains(res[1], "[i]")

	pattern = regexp.MustCompile(`\[c (.*?)]`)
	res = pattern.FindStringSubmatch(res[1])

	if res != nil {
		color = res[1]
	}

	return
}

func extractRelation(inputLine string) (string, string) {
	pattern := regexp.MustCompile(`\[\*](.*)?\[/\*]`)
	res := pattern.FindStringSubmatch(inputLine)
	if res == nil {
		return "", ""
	}

	if strings.Contains(res[1], "[ref]") {
		return "", cleanupLine(res[1])
	}

	return cleanupLine(res[1]), ""
}

func extractNote(inputLine string) (string, string) {
	pattern := regexp.MustCompile(`\[p.*?](\[c\s*(.*?)])?(.*?)\[/`)
	res := pattern.FindStringSubmatch(inputLine)
	if res != nil {
		return res[2], cleanupLine(res[3])
	}

	return "", ""
}

func cleanupLine(inputLine string) string {
	pattern := regexp.MustCompile(`\[.*?]`)
	result := pattern.ReplaceAllString(inputLine, "")

	pattern = regexp.MustCompile(`\s{2,}`)
	result = pattern.ReplaceAllString(result, " ")

	return strings.TrimSpace(result)
}

func scanWordArticleTitle(inputLine string) string {
	pattern := regexp.MustCompile(`^[^\s\[].*`)
	return pattern.FindString(inputLine)
}

func scanTitle(prefix, inputLine string, dest *string) bool {
	regexStr := fmt.Sprintf(`%s\s*"(.*)"`, prefix)
	pattern := regexp.MustCompile(regexStr)
	res := pattern.FindStringSubmatch(inputLine)
	if res != nil {
		*dest = res[1]
		return true
	}

	return false
}
