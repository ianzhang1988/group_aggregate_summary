package main

import (
	"fmt"
	. "grpaggsum"

	"github.com/go-gota/gota/dataframe"
)

type OriginData struct {
	isp  string
	loc  string
	city string
	Bar  int
	Foo  int
}

type AggExample struct {
	Bar     uint64
	Foo     uint64
	Counter uint64
}

func (a *AggExample) Add(od *OriginData) {
	a.Bar += uint64(od.Bar)
	a.Foo += uint64(od.Foo)
	a.Counter += 1
}

// add Self
func (a *AggExample) AddS(o *AggExample) {
	a.Bar += o.Bar
	a.Foo += o.Foo
	a.Counter += o.Counter
}

func (a *AggExample) AddPD(pd ProtocalData) {
	od := pd.Data.(*OriginData)
	a.Add(od)
}

func (a *AggExample) AddSelf(o AggI) {
	ot := o.(*AggExample)
	a.AddS(ot)
}

func NewAggExample() AggI {
	return &AggExample{}
}

func OriginDataIspLocCityUrl(pd ProtocalData) string {
	od := pd.Data.(*OriginData)
	url := fmt.Sprintf("%s/%s/%s", od.isp, od.loc, od.city)
	return url
}

func OriginDataBarUrl(pd ProtocalData) string {
	od := pd.Data.(*OriginData)
	url := ""
	if od.Bar > 2 {
		url = "bar>2"
	} else {
		url = "bar<=2"
	}

	return url
}

func FilterNumGt2(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
	dfnew := df.Filter( // or
		dataframe.F{Colname: "Bar", Comparator: ">", Comparando: 2.1},
		dataframe.F{Colname: "Foo", Comparator: ">", Comparando: 2.1},
	)
	return &dfnew, nil
}

func GroupByISP(df *dataframe.DataFrame) (*dataframe.DataFrame, error) {
	grouped := df.GroupBy("ISP")
	if grouped.Err != nil {
		return nil, grouped.Err
	}
	dfIsp := grouped.Aggregation(DfAggSumParam("Bar", "Foo", "Count"))
	if dfIsp.Err != nil {
		return nil, grouped.Err
	}
	return &dfIsp, nil
}

func CollectExapmleFromTreeDfView() *DFView {
	dfv := DFView{
		Keys:    []string{"ISP", "Loc", "Ver"},
		Columns: []string{"Bar", "Foo", "Count"},
		Prefix:  AggExampleIspLocCityT,
		Convert: func(v interface{}) map[string]float64 {
			example := v.(*AggExample)
			return map[string]float64{
				"Bar":   float64(example.Bar),
				"Foo":   float64(example.Foo),
				"Count": float64(example.Counter),
			}
		},
	}
	return &dfv
}

func CollectExapmleLevelFromTreeDfView() *DFView {
	dfv := DFView{
		Keys:    []string{"bar>2", "bar<=2"},
		Columns: []string{"Bar", "Foo", "Count"},
		Prefix:  AggExampleBarLevelT,
		Convert: func(v interface{}) map[string]float64 {
			example := v.(*AggExample)
			return map[string]float64{
				"Bar":   float64(example.Bar),
				"Foo":   float64(example.Foo),
				"Count": float64(example.Counter),
			}
		},
	}
	return &dfv
}
