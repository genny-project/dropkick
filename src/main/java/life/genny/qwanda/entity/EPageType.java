package life.genny.qwanda.entity;

public enum EPageType {



		DEFAULT("DEFAULT",EPageTypeCategory.RECORD),
		DAY("DAY",EPageTypeCategory.DATE),
		WEEK("WEEK",EPageTypeCategory.DATE),
		MONTH("MONTH",EPageTypeCategory.DATE),
		YEAR("YEAR",EPageTypeCategory.DATE),
		HOUR("HOUR",EPageTypeCategory.DATETIME),
		MINUTE("MINUTE",EPageTypeCategory.DATETIME),
		SECOND("SECOND",EPageTypeCategory.DATETIME),
		STATUS("STATUS",EPageTypeCategory.GROUP);

		private final String type;
		private final EPageTypeCategory category;
		
		EPageType(String value,EPageTypeCategory pageTypeCategory) {
			this.type = value;
			this.category = pageTypeCategory;
		}
		
		public String EPageType() {
			return type;
		}	
		
		public EPageTypeCategory getCategory()
		{
			return category;
		}
	}