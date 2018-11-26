package main

import (
	"bufio"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
 * 百万行数据的CSV，因为没有限制内存，所以直接将它们全部考进来
 * 如果每次使用流水，nextline的实现肯定太慢了，考虑partition
 * 对于t1.p1中每个元组，然后和t2.p1中的元组进行哈希join，找到
 * 适配的时候，加一个filter的过滤，然后再连接。
 *
 * partition的原则，每个partition中的，t1，t2的a的范围一致。
 *
 * 估算来看100w行，差不多是8MB大小的文件，1000w是80MB大小的文件
 * partition的时候，每个routine，分读1MB大小的页面进行hash表的
 * 创建
*/

type bucket struct {
	chain  []int
	rwlock sync.RWMutex
}

type hashTable struct {
	//读不用锁整个hash表，写需要锁整个哈希表
	table  map[int]*bucket
	rwlock sync.RWMutex
}

type table struct {
	tid   int
	size  int
	fd    *os.File
	finfo os.FileInfo
	data  *[]data
}

type data struct {
	a int
	b int
}

const (
	goroutineSize int64 = 1 << 10 //1MB
)

var partitionHashTable = NewHashTable()

func main() {
	tstart := time.Now()
	//load table info
	table1, err := loadInfoFromCSV(1, "t11.csv")
	if err != err {
		return
	}
	table2, err := loadInfoFromCSV(2, "t22.csv")
	if err != err {
		return
	}

	//judge which table is the bigger, table1 is always the smaller
	if table1.size > table2.size {
		table1, table2 = table2, table1
	}

	//test for table info
	//fmt.Println(table1)
	//fmt.Println(table2)

	/*
     *  scan phase，看成是scan节点
     */
	//根据table的size，小于一个1MB的，我们还是顺序读
	//if int64(tab.size) > goroutineSize{     //todo

	//}
	table1.data, err = table1.sequentialRead()
	if err != nil {
		fmt.Println("table1 data read error: ", err)
		return
	}
	table2.data, err = table2.sequentialRead()
	if err != nil {
		fmt.Println("table2 data read error: ", err)
		return
	}
	//fmt.Println(table1.data, table2.data)

	/*
	 *   build phase，看成是join节点的第一阶段
	 */
	data1 := table1.data
	len1 := len(*data1)
	wg := sync.WaitGroup{}
	wg.Add(4) //需要锁升级的时候，需要先放读锁，然后在加写锁
	go func() {
		partitionHashMap(data1, 0, len1/4)
		wg.Done()
	}()
	go func() {
		partitionHashMap(data1, len1/4, len1/2)
		wg.Done()
	}()
	go func() {
		partitionHashMap(data1, len1/2, len1*3/4)
		wg.Done()
	}()
	go func() {
		partitionHashMap(data1, len1*3/4, len1)
		wg.Done()
	}()
	wg.Wait()
	//test seeing hash table
	//partitionHashTable.showHashTable()

	/*
	 * Agg phase, 这个携程看成是Agg节点，从4个channel接受4个probe worker送来的流水数据
	*/

	streams := make([]chan int, 4)
	for i := 0; i < 4; i++ {
		streams[i] = make(chan int, 15) //为每个chan设置100的缓冲
	}
	var count int = 0


	fmt.Println("probe start")
	/*
	 * probe phase，看成join节点的第二阶段
	 * for the bigger size table, get every line to join with the ele in map
	 * since we want to get data stream, we use chan to transmit the temp result
	 * rather than return to partial count result totally
	 *
	 * filter的过程，我们加在了probe的过程里面
	 */
	data2 := table2.data
	len2 := len(*data2)
	wg.Add(4)
	go func() {
		partitionProbe(streams[0], table2, 0, len2/4)
		close(streams[0])
		wg.Done()
	}()
	go func() {
		partitionProbe(streams[1], table2, len2/4, len2/2)
		close(streams[1])
		wg.Done()
	}()
	go func() {
		partitionProbe(streams[2], table2, len2/2, len2*3/4)
		close(streams[2])
		wg.Done()
	}()
	go func() {
		partitionProbe(streams[3], table2, len2*3/4, len2)
		close(streams[3])
		wg.Done()
	}()

	//这边出现了死锁，因为管道中的数据没有人接受，还没有执行到下面的流程
	//go AggNode(streams, &workers, &count, done, wg)
	res := make(chan int, 4)
	wg.Add(5)
	go func() {
		AggNode1(streams[0], res)
		wg.Done()
	}()
	go func() {
		AggNode1(streams[1], res)
		wg.Done()
	}()
	go func() {
		AggNode1(streams[2], res)
		wg.Done()
	}()
	go func() {
		AggNode1(streams[3], res)
		wg.Done()
	}()
	go func() {
		AggNode2(res, &count)
		wg.Done()
	}()
	wg.Wait()



	/*
	  * show agg result
	*/
	fmt.Println(count)
	fmt.Println("time duration", time.Since(tstart))

}

/*
 * 死锁的原因是需要检测，chan的关闭，特别是Agg1和Agg2
 */

func AggNode1(stream chan int, res chan int){
	var pCount int = 0
	var off bool = false
	for{
		select {
		case _, ok:=<-stream:
			if !ok{
				//channel 关闭
				off = true
				break
			}
			pCount++
		}
		if off {
			break
		}
	}
	fmt.Println("pCount= ", pCount)
	res<-pCount
}

func AggNode2(res chan int, count *int){
	var ans int = 0
	var off bool = false
	for i:=0; i<4; i++{   //循环四次接受结果
		select {
		case one, ok:=<-res:
			if !ok{
				off = true
				break
			}else{
				fmt.Println("get res")
				ans += one
			}

		}
		if off {
			break
		}
	}
	*count = ans
}

/*
func AggNode(streams []chan int, worker,count *int, done chan bool, wg sync.WaitGroup){
	for{
		select {
		case <-streams[0]:
			//fmt.Println("get1")
			*count++
		case <-streams[1]:
			//fmt.Println("get1")
			*count++
		case <-streams[2]:
			//fmt.Println("get1")
			*count++
		case <-streams[3]:
			//fmt.Println("get1")
			*count++
		case <-done:
			*worker++
			if *worker==4 &&(len(streams[0])+len(streams[1])+len(streams[2])+len(streams[3]))<=0{
				break
			}
		default:
			if *worker==4 &&(len(streams[0])+len(streams[1])+len(streams[2])+len(streams[3]))<=0{
				break
			}
		}
	}
	wg.Done()
}
*/

func partitionProbe(stream chan int, tab *table, start, end int) {
	data2 := tab.data
	for i := start; i < end; i++ {
		buck, ok := partitionHashTable.table[(*data2)[i].a]
		if ok {
			//即使有bucket，由于这里我们加入了filter，所以需要进行判断
			/*
			 * filter phase 这个过滤，要么在join在的上层做，要么推导join自身节点在做
			 */
			len3 := len(buck.chain)
			for j := 0; j < len3; j++ {
				/*
				 * filter: t1.b >t2.b
				 */
				if tab.tid == 1 { //如果非hash的表是表1
					if (*data2)[i].b > buck.chain[j] {
						stream <- 1
						//fmt.Println("add1")
					}
				} else { //如果非hash的表是表2
					if buck.chain[j] > (*data2)[i].b {
						stream <- 1
						//fmt.Println("add1")
					}
				}
			}
		} else {
			//hash表中，没有join的bucket，直接放弃内表的元组
			continue
		}
	}
}

func partitionHashMap(data1 *[]data, start, end int) {
	for i := start; i < end; i++ {
		partitionHashTable.rwlock.RLock()
		buck, ok := partitionHashTable.table[(*data1)[i].a]
		if ok { //bucket添加元素，互斥锁
			buck.rwlock.Lock()
			buck.chain = append(buck.chain, (*data1)[i].b)
			buck.rwlock.Unlock()
			partitionHashTable.rwlock.RUnlock()
		} else { //map添加映射，互斥锁
			partitionHashTable.rwlock.RUnlock() //因为不能进行读锁升级，所以需要双重校验
			partitionHashTable.rwlock.Lock()
			buck, ok := partitionHashTable.table[(*data1)[i].a]
			if ok { //发现，在获取写锁的过程中已经被人添加
				buck.rwlock.Lock()
				buck.chain = append(buck.chain, (*data1)[i].b)
				buck.rwlock.Unlock()
			} else { //在我获取写锁的过程中，没有动过的话，再添加
				chain := make([]int, 1)
				chain[0] = (*data1)[i].b
				buck := bucket{}
				buck.chain = chain
				buck.rwlock = sync.RWMutex{}
				partitionHashTable.table[(*data1)[i].a] = &buck
			}
			partitionHashTable.rwlock.Unlock()
		}
	}
}

//way1：IO并行，可以提升速度
func (tab *table) concurrentRead(wg sync.WaitGroup) {
	size := int64(tab.size/4)
	wg.Add(10)
	rows := make([]chan data, 5)
	// concurrent read goroutines
	go func() {
		partitionRead(tab, rows[0], 0)
		wg.Done()
	}()
	go func() {
		partitionRead(tab, rows[1], size)
		wg.Done()
	}()
	go func() {
		partitionRead(tab, rows[2], size*2)
		wg.Done()
	}()
	go func() {
		partitionRead(tab, rows[3], size*3)
		wg.Done()
	}()
	go func() {
		partitionRead(tab, rows[4], size*4)
		wg.Done()
	}()
	//concurrent building-hash-table goroutines
	go func() {
		partitionBuild(rows[0])
		wg.Done()
	}()
	go func() {
		partitionBuild(rows[1])
		wg.Done()
	}()
	go func() {
		partitionBuild(rows[2])
		wg.Done()
	}()
	go func() {
		partitionBuild(rows[3])
		wg.Done()
	}()
	go func() {
		partitionBuild(rows[4])
		wg.Done()
	}()
	wg.Wait()
}

func partitionBuild(row chan data){
	var off bool = false
	for{
		select {
		case r, ok:=<-row:
			if !ok{
				off = true
				break
			}else{
				buildRow(r.a, r.b)
			}
		}
		if off{
			break
		}
	}
}

func buildRow(key, val int){
	partitionHashTable.rwlock.RLock()
	buck, ok := partitionHashTable.table[key]
	if ok { //bucket添加元素，互斥锁
		buck.rwlock.Lock()
		buck.chain = append(buck.chain, val)
		buck.rwlock.Unlock()
		partitionHashTable.rwlock.RUnlock()
	} else { //map添加映射，互斥锁
		partitionHashTable.rwlock.RUnlock() //因为不能进行读锁升级，所以需要双重校验
		partitionHashTable.rwlock.Lock()
		buck, ok := partitionHashTable.table[key]
		if ok { //发现，在获取写锁的过程中已经被人添加
			buck.rwlock.Lock()
			buck.chain = append(buck.chain, val)
			buck.rwlock.Unlock()
		} else { //在我获取写锁的过程中，没有动过的话，再添加
			chain := make([]int, 1)
			chain[0] = val
			buck := bucket{}
			buck.chain = chain
			buck.rwlock = sync.RWMutex{}
			partitionHashTable.table[key] = &buck
		}
		partitionHashTable.rwlock.Unlock()
	}
}

func partitionRead(tab *table, row chan data, logicalOffset int64) error{
	physicalOffset, err:= tab.getPhysicalOffset(logicalOffset)
	if err!=nil{
		fmt.Println("random read err")
		return errors.New("random read err")
	}
	//in readAtOffset func defined the reading size
	datas, err := tab.readAtOffset(physicalOffset)
	if err!=nil{
		return err
	}
	str := string(datas[:])
	res := strings.FieldsFunc(str, func(r rune) bool {
		return !(r >= '0' && r <= '9')
	})
	if len(res)%2!=0{
		fmt.Println("解析的数据不为偶数，有问题")
		return errors.New("解析的数据不为偶数，有问题")
	}
	strLen := len(res)
	for i:=0; i<strLen; i+=2 {
		a, err1 := strconv.Atoi(res[i])
		b, err2 := strconv.Atoi(res[i+1])
		if err1 != nil || err2 != nil {
			fmt.Println("解析失败，包含非法字符")
			return errors.New("解析失败，包含非法字符")
		}
		r := data{a,b}
		/*
		 * way1：
		 * dataBuf = append(dataBuf, row)
		 * 如果是放buf，那么读的时候是并行，看看能不能做成流水
		 */

		/*
		 * 直接将row扔到管道里面，扔给上层routine
		 */
		 row <- r
		 fmt.Println("put a row")
	}
	return nil
}

func (tab *table) getPhysicalOffset(offset int64) (pOffset int64, err error) {
	//需要看当前的前一个是不是\n，和当前自己是不是\n
	nowBuf := make([]byte, 2)
	if tab.finfo.Size() < offset {
		return 0, errors.New("offset bigger than file size")
	}
	//如果从头开始读，没毛病
	if offset == 0 {
		return 0, nil
	}
	//如果是从中间开始读，需要看当前是不是回车，当前的上一个是不是回车
	n, err := tab.fd.ReadAt(nowBuf, offset-1)
	if err != nil || n != 2 {
		return 0, errors.New("read fail")
	}
	if nowBuf[1] == '\n' {
		//说明下面一个正好是元组的开始
		return offset + 1, nil
	}
	if nowBuf[0] == '\n' {
		//说明当前正好是元组的开始
		return offset, nil
	}
	//否则的话，应该是处于元组的中间，需要读到回车终止
	nowBuf = make([]byte, 1)
	offset += 1
	for n, err := tab.fd.ReadAt(nowBuf, offset); err != nil && n == 1 && nowBuf[0] != '\n'; offset++ {
		//这些数据不需要读，已经被上一个协程读过
	}
	if nowBuf[0] != '\n' {
		return 0, errors.New("read fail in next")
	} else{
		return offset + 1, nil
	}
}

func (tab *table) readAtOffset(offset int64) (*[]byte, error) {
	//get real size of file remaining
	//size := goroutineSize
	size := int64(tab.size/4)
	if (tab.finfo.Size() - offset) < size {
		size = tab.finfo.Size() - offset
	}
	routinueBuf := make([]byte, size)
	n, err := tab.fd.ReadAt(routinueBuf, offset)
	if err != nil || int64(n) != size {
		return nil, errors.New("read fail in real read")
	}
	if routinueBuf[size-1] == '\n' {
		return &routinueBuf, nil
	}
	//如果没有读到文件换行，需要增加读，否则元组不完整
	appendBuf := make([]byte, 1)
	offset = offset + size
	for n, err := tab.fd.ReadAt(appendBuf, offset); err != nil && n == 1 && appendBuf[0] != '\n'; offset++ {
		routinueBuf = append(routinueBuf, appendBuf[0])
	}
	if appendBuf[0] != '\n' {
		return nil, errors.New("read fail in real read")
	}
	return &routinueBuf, nil
}



//way2：IO串行，但是build的时候，可以起worker来并行
func (tab *table) sequentialRead() (*[]data, error) {
	dataBuf := make([]data, 0)
	bfio := bufio.NewReader(tab.fd)
	var row data
	for {
		line, _, err := bfio.ReadLine()
		if err != nil {
			if err == io.EOF {
				//here read ok
				return &dataBuf, nil
			}
			return nil, err
		}
		str := strings.TrimSpace(string(line))
		res := strings.FieldsFunc(str, func(r rune) bool {
			return !(r >= '0' && r <= '9')
		})
		if len(res) != 2 {
			return nil, errors.New("data line analysis fail")
		}
		a, err1 := strconv.Atoi(res[0])
		b, err2 := strconv.Atoi(res[1])
		if err1 != nil || err2 != nil {
			return nil, errors.New("CSV contain invalid data")
		}
		row = data{a, b}
		dataBuf = append(dataBuf, row)
	}
}

func (tab *table) close() error {
	err := tab.fd.Close()
	return err
}

func (ht *hashTable) showHashTable() {
	fmt.Println("map size = ", len(ht.table))
	for k, v := range ht.table {
		fmt.Print(k, ":")
		for _, one := range v.chain {
			fmt.Print(" ", one)
		}
		fmt.Println()
	}
}

//每个goroutine会进行各自的哈希
func NewHashTable() (h hashTable) {
	h = hashTable{}
	h.table = make(map[int]*bucket)
	return h
}

func loadInfoFromCSV(tableid int, path string) (*table, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &table{tid: tableid, size: int(stat.Size()), fd: file, finfo: stat}, nil
}
