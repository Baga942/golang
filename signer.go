package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)


const MULTI_HASH_CONST = 6

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}
	input := make(chan interface{})
	for _, job := range jobs {
		//wg.Add до запуска горутины
		wg.Add(1)
		output := make(chan interface{})
		go workerAsync(job, input, output, wg)
		input = output
	}
	wg.Wait()
}

func workerAsync(job job, input, output chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(output)
	job(input, output)
}


func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	for i := range in {
		wg.Add(1)
		go singleHashJob(i, out, wg, mu)
	}
	wg.Wait()
}

func singleHashJob(in interface{}, out chan interface{}, wg *sync.WaitGroup, mu *sync.Mutex) {
	mu.Lock()
	md5data := DataSignerMd5(strconv.Itoa(in.(int)))
	mu.Unlock()
	hashChan := make(chan string)
	go func(output chan string, data interface{}) {
		output <- DataSignerCrc32(strconv.Itoa(data.(int)))
	}(hashChan, in)
	md5crc32data := DataSignerCrc32(md5data)
	crc32data := <-hashChan
	out <- crc32data + "~" + md5crc32data
	wg.Done()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for i := range in {
		wg.Add(1)
		go multiHashJob(i.(string), out, wg)
	}
	wg.Wait()
}


func multiHashJob(in string, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	mu := &sync.Mutex{}
	jobWg := &sync.WaitGroup{}
	combinedChunks := make([]string, MULTI_HASH_CONST)

	for i := 0; i < MULTI_HASH_CONST; i++ {
		jobWg.Add(1)
		data := strconv.Itoa(i) + in

		go func(acc []string, index int, data string, jobWg *sync.WaitGroup, mu *sync.Mutex) {
			defer jobWg.Done()
			data = DataSignerCrc32(data)
			mu.Lock()
			acc[index] = data
			mu.Unlock()
		}(combinedChunks, i, data, jobWg, mu)
	}
	jobWg.Wait()
	out <- strings.Join(combinedChunks, "")
}

func CombineResults(in, out chan interface{}) {
	var slice []string
	for data := range in {
		slice = append(slice, data.(string))
	}
	sort.Strings(slice)
	res := strings.Join(slice, "_")
	out <- res
}



// сюда писать код