package main

import (
	"fmt"
	"log"
	"os"

	"github.com/armon/go-radix"
	"github.com/go-gota/gota/dataframe"

	. "grpaggsum"
)

const (
	AggExampleIspLocCityT = "agg_example/isp_loc_city/"
	AggExampleBarLevelT   = "agg_example/Barlevel/"
)

func WriteCSV(df *dataframe.DataFrame, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return df.WriteCSV(file)
}

func DummyData(pd chan ProtocalData) {
	pd <- ProtocalData{Data: &OriginData{Bar: 1, Foo: 1, isp: "ct", loc: "bj", city: "bj"}}
	pd <- ProtocalData{Data: &OriginData{Bar: 2, Foo: 2, isp: "ct", loc: "bj", city: "bj"}}
	pd <- ProtocalData{Data: &OriginData{Bar: 3, Foo: 3, isp: "cnc", loc: "sd", city: "wf"}}
	pd <- ProtocalData{Data: &OriginData{Bar: 4, Foo: 4, isp: "cnc", loc: "sd", city: "qd"}}
	pd <- ProtocalData{Data: &OriginData{Bar: 5, Foo: 5, isp: "cnc", loc: "sd", city: "wf"}}
	close(pd)
}

func main() {

	Log := log.New(os.Stdout, "[info]", log.Lshortfile|log.Ltime|log.Ldate)

	dataChan := make(chan ProtocalData)
	go DummyData(dataChan)

	// 用radix tree作为全局汇总数据容器
	r := radix.New()

	// 根据url的维度分别汇聚数据
	orch := AggOrchestrator{Trie: r}
	// Register(p1, p2, p3)
	// p1: 根据逻辑解析输入数据，生成聚合维度的url
	// p2: new一个新的聚合器，来承接数据
	// p3: 聚合器的类型，作为p1中url的前缀，用来提取本类型数据
	orch.Register(OriginDataIspLocCityUrl, NewAggExample, AggExampleIspLocCityT)
	// 分类汇聚可以通过url实现， 如 bar >2, <=2这类的汇聚
	orch.Register(OriginDataBarUrl, NewAggExample, AggExampleBarLevelT)

	// 从数据队列中读取数据并进行聚合
	for d := range dataChan {
		if d.Err != nil {
			fmt.Println("protacal data err:", d.Err)
			break
		}

		orch.Process(d)
	}

	// 根据需求从tree中聚合数据, 组成dataframe，后面可以从这个dataframe中过滤或者group出不同维度的数据
	sumOrch := NewSumOrchestrator(r)

	sumOrch.RegisterDfView("example", CollectExapmleFromTreeDfView())
	sumOrch.RegisterDfView("examplelevel", CollectExapmleFromTreeDfView())
	sumOrch.RegisterDfProcedure("num>2", FilterNumGt2)
	sumOrch.RegisterDfProcedure("group_by_isp", GroupByISP)

	df, err := sumOrch.GetView("example")
	if err != nil {
		Log.Fatal(err)
	}
	err = WriteCSV(df, "example.csv")
	if err != nil {
		Log.Fatal(err)
	}

	df, err = sumOrch.GetView("examplelevel")
	if err != nil {
		Log.Fatal(err)
	}
	err = WriteCSV(df, "examplelevel.csv")
	if err != nil {
		Log.Fatal(err)
	}

	// 过滤出有流量的行
	dfGt0, err := sumOrch.Process("example", "num>2")
	if err != nil {
		Log.Fatal(err)
	}
	err = WriteCSV(dfGt0, "exampleGt2.csv")
	if err != nil {
		Log.Fatal(err)
	}

	// group by isp
	dfIsp, err := sumOrch.Process("example", "group_by_isp")
	if err != nil {
		Log.Fatal(err)
		return
	}
	err = WriteCSV(dfIsp, "exampleISP.csv")
	if err != nil {
		Log.Fatal(err)
		return
	}
}
