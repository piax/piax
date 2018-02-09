package test.async;

import java.io.Serializable;

public class CSFContent implements Serializable {
		/**
	 * 
	 */
	private static final long serialVersionUID = 2223014752256952141L;
		public Long deadline;
		public Long period;
		public CSFContent(Long deadline, Long period) {
			this.deadline = deadline;
			this.period = period;
		}
}