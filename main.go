package main

import (
	"fmt"
	"sync"
	"time"
)

func orDone(done chan interface{}, input chan interface{}) chan interface{} {
	output := make(chan (interface{}))
	go func() {
		defer close(output)
		for {
			select {
			case <-done:
				return
			case in, open := <-input:
				if !open {
					return
				}
				select {
				case <-done:
					return
				case output <- in:
				}
			}
		}
	}()
	return output
}

func delayed(delay time.Duration) stepFunction {
	withDelay := func(done chan interface{}, input chan interface{}) chan interface{} {
		output := make(chan (interface{}))
		go func() {
			defer close(output)
			for in := range orDone(done, input) {
				time.Sleep(delay)
				output <- in
			}
		}()
		return output
	}
	return withDelay
}

func getMultiplexChannel(done chan interface{}, input chan interface{}, step stepFunction, count int) chan interface{} {
	output := make(chan interface{})

	var wg sync.WaitGroup
	wg.Add(count)

	go func() {
		defer close(output)
		for i := 0; i < count; i++ {
			stepInput := make(chan interface{})
			stepOutput := step(done, stepInput)

			go func(stepInput chan interface{}, stepOutput chan interface{}) {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					case in, opened := <-input:
						if !opened {
							return
						}
						stepInput <- in
						select {
						case <-done:
							return
						case output <- <-stepOutput:
						}
					}
				}
			}(stepInput, stepOutput)
		}
		wg.Wait()
	}()

	return output
}

type stepFunction func(chan interface{}, chan interface{}) chan interface{}

type Pipeline struct {
	done          chan interface{}
	input         chan interface{}
	stepFunctions []stepFunction
}

func NewPipeline(done chan interface{}) *Pipeline {
	pipeline := Pipeline{
		done:          done,
		stepFunctions: make([]stepFunction, 0),
	}
	return &pipeline
}

func FromStepFunction(done chan interface{}, step stepFunction) *Pipeline {
	p := NewPipeline(done)
	p.AddStep(step)
	return p
}

func (p *Pipeline) Cancel() {
	close(p.done)
}

func (p *Pipeline) AddStep(sf stepFunction) *Pipeline {
	p.stepFunctions = append(p.stepFunctions, sf)
	return p
}

func (p *Pipeline) AddStepConcurrent(sf stepFunction, count int) *Pipeline {
	multiplexedSf := func(done chan interface{}, input chan interface{}) chan interface{} {
		return getMultiplexChannel(done, input, sf, count)
	}
	p.AddStep(multiplexedSf)
	return p
}

func (p *Pipeline) AddPipelineFromPipeline(previousPipeline *Pipeline) *Pipeline {
	sfFromPipeline := func(done chan interface{}, input chan interface{}) chan interface{} {
		output := make(chan interface{})
		go func() {
			defer close(output)
			for in := range orDone(done, p.input) {
				output <- in
			}
		}()
		return output
	}
	p.AddStep(sfFromPipeline)
	p.SetInput(previousPipeline.Start())
	return p
}

func (p *Pipeline) SetInput(input chan interface{}) *Pipeline {
	p.input = input
	return p
}

func (p *Pipeline) Start() chan interface{} {
	if p.input == nil {
		panic("input not defined. Pipeline can not start")
	}
	output := p.input

	for i := 0; i < len(p.stepFunctions); i++ {
		output = p.stepFunctions[i](p.done, output)
	}
	return output
}

func simpleDelay(delay time.Duration) stepFunction {
	return func(done chan interface{}, input chan interface{}) chan interface{} {
		output := make(chan (interface{}))
		go func() {
			defer close(output)
			for in := range orDone(done, input) {
				time.Sleep(delay)
				output <- in
			}
		}()
		return output
	}
}

func bisect(done chan interface{}, input chan interface{}) (chan interface{}, chan interface{}) {
	output1 := make(chan interface{})
	output2 := make(chan interface{})

	go func() {
		defer close(output1)
		defer close(output2)

		for in := range orDone(done, input) {
			var output1, output2 = output1, output2
			for i := 0; i < 2; i++ {
				select {
				case <-done:
					return
				case output1 <- in:
					output1 = nil
				case output2 <- in:
					output2 = nil
				}
			}
		}
	}()

	return output1, output2
}

func JoinPipelines(done chan interface{}, pipeline1 *Pipeline, pipeline2 *Pipeline) *Pipeline {
	p := NewPipeline(done)
	stepFunction := func(done chan interface{}, input chan interface{}) chan interface{} {
		output := make(chan interface{})

		go func() {
			defer close(output)
			for in := range orDone(done, input) {
				output <- in
			}
		}()

		return output
	}
	orChannel := or(done, pipeline1.Start(), pipeline2.Start())
	p.AddStep(stepFunction)
	p.SetInput(orChannel)
	return p
}

func or(done chan interface{}, input1 chan interface{}, input2 chan interface{}) chan interface{} {
	output := make(chan interface{})

	var wg sync.WaitGroup
	wg.Add(2)

	checkInput := func(channel chan interface{}) {
		defer wg.Done()
		for in := range orDone(done, channel) {
			output <- in
		}
	}

	go func() {
		defer close(output)

		go checkInput(input1)
		go checkInput(input2)

		wg.Wait()
	}()

	return output
}

func simpleAppender(text string) stepFunction {
	return func(done chan interface{}, input chan interface{}) chan interface{} {
		output := make(chan interface{})

		go func() {
			defer close(output)
			for in := range orDone(done, input) {
				if val, ok := in.(string); ok {
					output <- val + "-" + text
				}
			}
		}()

		return output
	}
}

func createInputStream(input chan interface{}, strings []string) {
	for _, s := range strings {
		input <- s
	}
	close(input)
}

func main() {
	done := make(chan interface{})

    /*
                               |-concurrent-|
        InputA -> [(a1)->(a2)->(2x(slowStep))->(a3)] -> [(b1)->(b2)]-\
                                                                      \
                                                                       -> [(j1)] -> END
                                                                      /
                               InputC -> [(c1)->(c2)->[1x(slowStep)]-/
    */

	// --- STREAM A from Input channel ---
	inputA := make(chan interface{})
	go createInputStream(inputA, []string{"A", "B", "C", "D", "E", "F", "G"})

	slowStep := delayed(time.Duration(2 * time.Second))

	pipelineA := NewPipeline(done).
		SetInput(inputA).
		AddStep(simpleAppender("a1")).
		AddStep(simpleAppender("a2")).
		AddStepConcurrent(slowStep, 2).
		AddStep(simpleAppender("a3"))

	// --- STREAM B from pipeline A ---
	pipelineB := NewPipeline(done).
		AddPipelineFromPipeline(pipelineA).
		AddStep(simpleAppender("b1")).
		AddStep(simpleAppender("b2"))

	// --- STREAM C from input C ---
	inputC := make(chan interface{})

	go createInputStream(inputC, []string{"D", "E", "F", "G"})
	pipelineC := NewPipeline(done).
		SetInput(inputC).
		AddStep(simpleAppender("c1")).
		AddStep(simpleAppender("c2")).
		AddStep(slowStep)

	// --- Join ---
	joinedPipeline := JoinPipelines(done, pipelineB, pipelineC).
		AddStep(simpleAppender("j1"))
	joinedOutput := joinedPipeline.Start()

	for {
		select {
		case x, opened := <-joinedOutput:
			if !opened {
				return
			}
			fmt.Printf("Received: %s\n", x)
		}
	}
}
