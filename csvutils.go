package csvutils

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"

	// "github.com/lujiacn/rservcli"

	"github.com/tealeg/xlsx"
)

//SliceToCsvStr
func SliceToCsvStr(colNames []string, rows [][]string) (string, error) {
	output := strings.Join(colNames, ",")
	for _, row := range rows {
		row_str := strings.Join(row, ",")
		output = output + row_str + "\n"
	}
	return output, nil

}

//SliceToMap convert slice to map
func SliceToMap(colNames []string, rows [][]string) ([]map[string]string, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	colNum := len(colNames)
	rowNum := len(rows)

	// create maps
	results := []map[string]string{}
	for i := 0; i < rowNum; i++ {
		if len(rows[i]) != colNum {
			// fmt.Println("row num:", i)
			// fmt.Println("Col len:", len(rows[i]))
			// fmt.Println("ColName len:", colNum)
			return nil, errors.New("Error: ColNames length and record length not consistent.")
		}
		t_map := map[string]string{}
		for j := 0; j < colNum; j++ {
			t_map[colNames[j]] = rows[i][j]
		}
		results = append(results, t_map)
	}

	return results, nil
}

//SliceToJson return []byte
func SliceToJson(colNames []string, rows [][]string) ([]byte, error) {
	results, err := SliceToMap(colNames, rows)
	if err != nil {
		return nil, err
	}
	output, err := json.Marshal(results)
	if err != nil {
		return nil, err
	}
	return output, nil
}

//CSV to map[string]string converter
func ReadCsvToMap(file_name string) ([]map[string]string, error) {
	colNames, rawData, err := ReadCsvToSlice(file_name)
	if err != nil {
		return nil, err
	}
	return SliceToMap(colNames, rawData)

}

//Read csv file return json bytes
func ReadCsvToJson(file_name string) ([]byte, error) {
	results, err := ReadCsvToMap(file_name)
	if err != nil {
		return nil, err
	}
	output, err := json.Marshal(results)
	if err != nil {
		return nil, err
	}
	return output, nil
}

//Excle file to map[string]string converter
func ReadXlsToMap(file_name string) ([]map[string]string, error) {
	colNames, rawData, err := ReadXlsToSlice(file_name)
	if err != nil {
		return nil, err
	}
	return SliceToMap(colNames, rawData)
}

func ReadCsvToSlice(file_name string) ([]string, [][]string, error) {
	//open File
	file, err := os.Open(file_name)
	if err != nil {
		return nil, nil, err
	}
	//Create *Reader
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, err
	}

	// fmt.Println("records length", len(records))
	if len(records) == 0 {
		return nil, nil, nil
	}
	colNames := records[0]
	rawData := records[1:len(records)]
	return colNames, rawData, err
}

func ReadXlsToSlice(file_name string) ([]string, [][]string, error) {
	//open File
	xlFile, err := xlsx.OpenFile(file_name)
	if err != nil {
		return nil, nil, err
	}
	colNames := []string{}
	results := [][]string{}
	sheet := xlFile.Sheets[0] //only read one sheet
	for r_n, row := range sheet.Rows {
		if r_n == 0 {
			// fmt.Println(row.Cells)
			for _, cell := range row.Cells {
				cellValue, _ := cell.String()
				colNames = append(colNames, cellValue)
			}
		} else {
			slice_row := []string{}
			for _, cell := range row.Cells {
				cellValue, _ := cell.String()
				slice_row = append(slice_row, cellValue)
			}
			// fmt.Println(map_row)
			results = append(results, slice_row)
		}

	}
	return colNames, results, err
}

//Read CSV to channel by row
func ReadCsvToArrayCh(file_name string) (resultC chan interface{}) {
	resultC = make(chan interface{})

	// fmt.Println("In read csv to array chan")
	//open File
	file, err := os.Open(file_name)
	if err != nil {
		resultC <- err
		close(resultC)
		return
	}

	//Create *Reader
	reader := csv.NewReader(file)
	if err != nil {
		resultC <- err
		close(resultC)
		return
	}
	reader.Read() //avoid header
	go func() {
		for {
			// fmt.Println("In go loop")
			record, err := reader.Read()
			if err == io.EOF {
				break
			}

			if err != nil {
				resultC <- err
				close(resultC)
				return
			}
			resultC <- record
		}
		close(resultC)
	}()
	return resultC
}

func ReadCsvColNames(file_name string) ([]string, error) {
	//open File
	file, err := os.Open(file_name)
	if err != nil {
		return nil, err
	}
	//Create *Reader
	reader := csv.NewReader(file)
	if err != nil {
		return nil, err
	}
	record, err := reader.Read()
	if err == io.EOF {
		return nil, errors.New("Blank file")
	}
	if err != nil {
		return nil, err
	}
	return record, nil
}

func ReadXlsColNames(file_name string) ([]string, error) {
	xlFile, err := xlsx.OpenFile(file_name)
	if err != nil {
		return nil, err
	}
	colNames := []string{}
	sheet := xlFile.Sheets[0] //only read one sheet
	row := sheet.Rows[0]
	for _, cell := range row.Cells {
		cellValue, _ := cell.String()
		colNames = append(colNames, cellValue)
	}
	return colNames, nil
}

func ReadXlsToArrayCh(file_name string) (resultC chan interface{}) {
	resultC = make(chan interface{})
	//open File
	xlFile, err := xlsx.OpenFile(file_name)
	if err != nil {
		resultC <- err
		close(resultC)
		return
	}
	sheet := xlFile.Sheets[0] //only read one sheet
	go func() {
		for r_n, row := range sheet.Rows {
			if r_n != 0 {
				slice_row := []string{}
				for _, cell := range row.Cells {
					cellValue, _ := cell.String()
					slice_row = append(slice_row, cellValue)
				}
				// fmt.Println(map_row)
				resultC <- slice_row
			}
		}
		close(resultC)
	}()
	return
}

//ColNameReplace replace . with _  due to mongo restriction
func ColNameReplace(colNames []string) []string {
	newColNames := []string{}
	for _, colName := range colNames {
		newColName := strings.Replace(colName, ".", "_", -1)
		newColNames = append(newColNames, newColName)
	}
	return newColNames
}
