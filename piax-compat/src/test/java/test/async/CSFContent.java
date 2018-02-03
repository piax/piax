package test.async;

import java.io.Serializable;

public class CSFContent implements Serializable {
		/**
	 * 
	 */
	private static final long serialVersionUID = 2223014752256952141L;
		public Serializable topic;
		public Long deadline;
		public Long period;
		public CSFContent(Serializable topic, Long deadline, Long period) {
			this.topic = topic;
			this.deadline = deadline;
			this.period = period;
		}
}