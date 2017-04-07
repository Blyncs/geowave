package mil.nga.giat.geowave.mapreduce.operations;

import com.beust.jcommander.Parameter;

public class CopyCommandOptions
{
	@Parameter(names = "--minSplits", description = "The min partitions for the input data")
	private Integer minSplits;

	@Parameter(names = "--maxSplits", description = "The max partitions for the input data")
	private Integer maxSplits;

	@Parameter(names = "--numThreads", description = "Number of threads to use", required = true)
	private Integer numThreads;

	// Default constructor
	public CopyCommandOptions() {

	}

	public CopyCommandOptions(
			final Integer minSplits,
			final Integer maxSplits,
			final Integer numThreads ) {
		this.minSplits = minSplits;
		this.maxSplits = maxSplits;
		this.numThreads = numThreads;
	}

	public Integer getMinSplits() {
		return minSplits;
	}

	public Integer getMaxSplits() {
		return maxSplits;
	}

	public Integer getNumThreads() {
		return numThreads;
	}

	public void setMinSplits(
			Integer minSplits ) {
		this.minSplits = minSplits;
	}

	public void setMaxSplits(
			Integer maxSplits ) {
		this.maxSplits = maxSplits;
	}

	public void setNumThreads(
			Integer numThreads ) {
		this.numThreads = numThreads;
	}
}
