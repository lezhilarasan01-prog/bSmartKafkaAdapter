/**
 * 
 */
package com.bcits.dataCollection;

import java.util.HashMap;

/**
 * @author Tarik
 *
 */
public class SequenceTracker extends HashMap<String, Long> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static StringBuilder builder = new StringBuilder();

	private static SequenceTracker seqTracker;

	public static SequenceTracker getInstance() {
		if (seqTracker == null)
			seqTracker = new SequenceTracker();
		return seqTracker;
	}

	private Long seq_id;

	public void addSequenceByType(String type, Long seq) {
		seqTracker.put(type,seq);
//		seqTracker.put(type, seqTracker.getOrDefault(type, seq));
	}

	public void reset() {
		seqTracker.clear();
		builder.setLength(0);
	}

	public String getQuery() {

		builder.setLength(0);
		builder.append("insert into meter_data.max_sequence (time_stamp,type,seq_id) VALUES ");

		seqTracker.forEach((type, seq) -> {
			this.seq_id = seq;
			builder.append("(now(),'" + type + "', " + seq + " ),");
//				System.out.println("key - "+type+" Value- "+seq);
		});
		return builder.toString().substring(0, builder.length() - 1) + " ON CONFLICT(type) " + "DO UPDATE "
				+ "SET time_stamp =  now(),seq_id=" + this.seq_id + " ";
	}
	
	
	public String getHesWiseQuery(String hes) {

		builder.setLength(0);
		builder.append("insert into meter_data.max_sequence (time_stamp,type,seq_id,hes) VALUES ");

		seqTracker.forEach((type, seq) -> {
			this.seq_id = seq;
			builder.append("(now(),'" + type + "', " + seq + ", '" + hes + "' ),");
//				System.out.println("key - "+type+" Value- "+seq+" hes- "+hes);
		});
		return builder.toString().substring(0, builder.length() - 1) + " ON CONFLICT(type,hes) " + "DO UPDATE "
				+ "SET time_stamp =  now(),seq_id=" + this.seq_id + " ";
	}

}
