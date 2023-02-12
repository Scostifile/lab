package raft

import "fmt"

type Entry struct {
	Term    int
	Command interface{}
}

func (e Entry) String() string {
	return fmt.Sprintf("T %v", e.Term)
}

type Log struct {
	Entries []Entry
	Index0  int
}

func mkLogEmpty() Log {
	return Log{make([]Entry, 1), 0}
}

func mkLog(log []Entry, index0 int) Log {
	return Log{log, index0}
}

func (l *Log) append(e Entry) {
	l.Entries = append(l.Entries, e)
}

func (l *Log) start() int {
	return l.Index0
}

func (l *Log) cutend(index int) {
	l.Entries = l.Entries[0 : index-l.Index0]
}

func (l *Log) cutstart(index int) {
	l.Index0 += index
	l.Entries = l.Entries[index:]
}

func (l *Log) slice(index int) []Entry {
	return l.Entries[index-l.Index0:]
}

func (l *Log) lastindex() int {
	return l.Index0 + len(l.Entries) - 1
}

func (l *Log) entry(index int) *Entry {
	return &(l.Entries[index-l.Index0])
}

func (l *Log) lastentry() *Entry {
	return l.entry(l.lastindex())
}
