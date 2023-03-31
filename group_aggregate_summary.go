package grpaggsum

import (
	"fmt"
	"log"
	"strings"

	"github.com/armon/go-radix"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

// input data
type ProtocalData struct {
	Type int
	Err  error
	Data interface{}
}

// aggregater
type AggI interface {
	AddPD(ProtocalData)
	AddSelf(AggI)
}

type NewAggtor func() AggI

// aggreate orchestrator
type ParseTreeUrl func(pd ProtocalData) string

type ParseUrlAndAggtor struct {
	parseUrl  ParseTreeUrl
	newAggtor NewAggtor
	aggType   string
}

type AggOrchestrator struct {
	Trie       *radix.Tree
	AggtorList []ParseUrlAndAggtor
}

func (o *AggOrchestrator) Register(parseUrl ParseTreeUrl, newAggtor NewAggtor, aggType string) {
	o.AggtorList = append(o.AggtorList, ParseUrlAndAggtor{
		parseUrl:  parseUrl,
		newAggtor: newAggtor,
		aggType:   aggType,
	})
}

func (o *AggOrchestrator) Process(pd ProtocalData) {
	// 可以给每个AggtorList中的聚合器一个线程来增加处理性能，或者多进程
	for _, a := range o.AggtorList {
		url := a.parseUrl(pd)
		url = a.aggType + url

		if ai, ok := o.Trie.Get(url); !ok {
			agg := a.newAggtor()
			agg.AddPD(pd)
			o.Trie.Insert(url, agg)
		} else {
			agg := ai.(AggI)
			agg.AddPD(pd)
		}
	}
}

// collect dataframe view or data from tree
type DFView struct {
	Keys    []string
	Columns []string
	Prefix  string
	Convert AggtorMap2Col
}

type AggtorMap2Col func(interface{}) map[string]float64

func (c *DFView) Collect(r *radix.Tree) (*dataframe.DataFrame, error) {
	prefix := c.Prefix
	convert := c.Convert

	KeyS := []series.Series{}
	for _, k := range c.Keys {
		KeyS = append(KeyS, series.New([]string{}, series.String, k))
	}
	ColumnS := []series.Series{}
	for _, c := range c.Columns {
		ColumnS = append(ColumnS, series.New([]string{}, series.Float, c))
	}

	r.WalkPrefix(prefix, func(key string, v interface{}) bool {
		keys := strings.Split(key[len(prefix):], "/")
		if len(keys) != len(c.Keys) {
			log.Println(keys, " and ", c.Keys, "mismatch")
			return false
		}

		for i, _ := range KeyS {
			KeyS[i].Append(keys[i])
		}

		col := convert(v)
		for i := range ColumnS {
			name := c.Columns[i]
			if v, ok := col[name]; ok {
				ColumnS[i].Append(v)
			} else {
				log.Printf("column %s mismatch", name)
			}
		}

		return false
	})

	df := dataframe.New(append(KeyS, ColumnS...)...)
	return &df, nil
}

// sum orchestrator
type SumProcedure func(*dataframe.DataFrame) (*dataframe.DataFrame, error)
type SumOrchestrator struct {
	Trie       *radix.Tree
	Views      map[string]*DFView
	Collected  map[string]*dataframe.DataFrame
	Procedures map[string]SumProcedure
}

func NewSumOrchestrator(r *radix.Tree) SumOrchestrator {
	return SumOrchestrator{
		Trie:       r,
		Views:      map[string]*DFView{},
		Collected:  map[string]*dataframe.DataFrame{},
		Procedures: map[string]SumProcedure{},
	}
}

func (so *SumOrchestrator) RegisterDfView(name string, dv *DFView) {
	so.Views[name] = dv
}

func (so *SumOrchestrator) RegisterDfProcedure(name string, p SumProcedure) {
	so.Procedures[name] = p
}

func (so *SumOrchestrator) GetView(name string) (*dataframe.DataFrame, error) {
	var df *dataframe.DataFrame
	var ok bool
	if df, ok = so.Collected[name]; ok {
		return df, nil
	}

	var View *DFView
	if View, ok = so.Views[name]; !ok {
		return nil, fmt.Errorf("view[%s] not registered", name)
	}

	df, err := View.Collect(so.Trie)
	if err != nil {
		return nil, err
	}

	return df, nil
}

func (so *SumOrchestrator) Process(view, procedure string) (*dataframe.DataFrame, error) {
	var Procedure SumProcedure
	var ok bool

	if Procedure, ok = so.Procedures[procedure]; !ok {
		return nil, fmt.Errorf("procedure[%s] not registered", procedure)
	}

	df, err := so.GetView(view)
	if err != nil {
		return nil, err
	}

	df, err = Procedure(df)
	if err != nil {
		return nil, err
	}

	return df, nil
}

// helper
func DfAggSumParam(collumn ...string) (typs []dataframe.AggregationType, colnames []string) {
	for _, c := range collumn {
		typs = append(typs, dataframe.Aggregation_SUM)
		colnames = append(colnames, c)
	}
	return typs, colnames
}
