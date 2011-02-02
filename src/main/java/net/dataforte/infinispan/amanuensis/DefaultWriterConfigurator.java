package net.dataforte.infinispan.amanuensis;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogMergePolicy;

public class DefaultWriterConfigurator implements WriterConfigurator {
	double maxMergeMB = 8;
	double minMergeMB = 8;
	private int maxMergeDocs = LogMergePolicy.DEFAULT_MAX_MERGE_DOCS;
	private int mergeFactor = LogMergePolicy.DEFAULT_MERGE_FACTOR;
	private boolean calibrateSizeByDeletes = true;
	private double noCFSRatio = LogMergePolicy.DEFAULT_NO_CFS_RATIO;
	private boolean useCompoundDocStore = true;
	private boolean useCompoundFile = true;

	public double getMaxMergeMB() {
		return maxMergeMB;
	}

	public void setMaxMergeMB(double maxMergeMB) {
		this.maxMergeMB = maxMergeMB;
	}

	public double getMinMergeMB() {
		return minMergeMB;
	}

	public void setMinMergeMB(double minMergeMB) {
		this.minMergeMB = minMergeMB;
	}

	public int getMaxMergeDocs() {
		return maxMergeDocs;
	}

	public void setMaxMergeDocs(int maxMergeDocs) {
		this.maxMergeDocs = maxMergeDocs;
	}

	public int getMergeFactor() {
		return mergeFactor;
	}

	public void setMergeFactor(int mergeFactor) {
		this.mergeFactor = mergeFactor;
	}

	public boolean isCalibrateSizeByDeletes() {
		return calibrateSizeByDeletes;
	}

	public void setCalibrateSizeByDeletes(boolean calibrateSizeByDeletes) {
		this.calibrateSizeByDeletes = calibrateSizeByDeletes;
	}

	public double getNoCFSRatio() {
		return noCFSRatio;
	}

	public void setNoCFSRatio(double noCFSRatio) {
		this.noCFSRatio = noCFSRatio;
	}

	public boolean isUseCompoundDocStore() {
		return useCompoundDocStore;
	}

	public void setUseCompoundDocStore(boolean useCompoundDocStore) {
		this.useCompoundDocStore = useCompoundDocStore;
	}

	public boolean isUseCompoundFile() {
		return useCompoundFile;
	}

	public void setUseCompoundFile(boolean useCompoundFile) {
		this.useCompoundFile = useCompoundFile;
	}

	@Override
	public void configure(IndexWriter writer) {
		LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy(writer);
		mergePolicy.setMaxMergeMB(maxMergeMB);
		mergePolicy.setMinMergeMB(minMergeMB);
		mergePolicy.setMaxMergeDocs(maxMergeDocs);
		mergePolicy.setMergeFactor(mergeFactor);
		mergePolicy.setCalibrateSizeByDeletes(calibrateSizeByDeletes);
		mergePolicy.setNoCFSRatio(noCFSRatio);
		mergePolicy.setUseCompoundDocStore(useCompoundDocStore);
		mergePolicy.setUseCompoundFile(useCompoundFile);
		writer.setMergePolicy(mergePolicy);
	}

}
