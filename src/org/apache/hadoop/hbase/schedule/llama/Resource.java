package org.apache.hadoop.hbase.schedule.llama;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class Resource {

	private Set<String> preferLocations;
	private boolean isFlexible;
	private int totalCpu;
	private int totalMem;
	private AtomicInteger requireCpu;
	private AtomicInteger requireMem;
	
	public Resource(String[] locations, int cpu, int memory, boolean isFlexible) {
		this.preferLocations = new HashSet<String>();
		this.totalCpu = cpu;
		this.totalMem = memory;
		this.requireCpu = new AtomicInteger(cpu);
		this.requireMem = new AtomicInteger(memory);
		this.isFlexible = isFlexible;
		
		for (String location : locations) {
			preferLocations.add(location);
		}
	}

	public Set<String> getPreferLocations() {
		return preferLocations;
	}

	public boolean isFlexible() {
		return isFlexible;
	}

	public int getTotalCpu() {
		return totalCpu;
	}

	public int getTotalMem() {
		return totalMem;
	}

	public AtomicInteger getRequireCpu() {
		return requireCpu;
	}

	public AtomicInteger getRequireMem() {
		return requireMem;
	}
	
	public void allocate(String location, int cpu, int memory) {
		this.requireCpu.addAndGet(-cpu);
		this.requireMem.addAndGet(-memory);
	}
	
	public void release(String location, int cpu, int memory) {
		this.requireCpu.addAndGet(cpu);
		this.requireMem.addAndGet(memory);
	}
	
	public boolean isAllocated() {
		if (requireCpu.get() > 0 || requireMem.get() > 0) {
			return false;
		}
		else {
			return true;
		}
	}
}
