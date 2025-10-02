-- Dynamic semantic view creation that adapts to environment
-- Uses the database and schema from the Snowflake CLI connection
create or replace semantic view DASH_DB_SI.RETAIL.SALES_ANALYST_CICD
	tables (
		MARKETING_CAMPAIGN_METRICS,
		PRODUCTS
	)
	facts (
		MARKETING_CAMPAIGN_METRICS.CLICKS as CLICKS with synonyms=('click_throughs','selections','hits','taps','activations') comment='The total number of times users clicked on an advertisement or promotional link as part of a marketing campaign.',
		MARKETING_CAMPAIGN_METRICS.IMPRESSIONS as IMPRESSIONS with synonyms=('views','ad_views','ad_exposures','display_count','ad_impressions','exposures','ad_views_count','views_count') comment='The total number of times an ad was displayed to users during a marketing campaign.',
		PRODUCTS.PRODUCT_ID as PRODUCT_ID with synonyms=('product_key','item_id','product_code','product_number','item_number','product_identifier') comment='Unique identifier for each product in the catalog.'
	)
	dimensions (
		MARKETING_CAMPAIGN_METRICS.CAMPAIGN_NAME as CAMPAIGN_NAME with synonyms=('advertising_campaign','marketing_initiative','promotional_name','campaign_title','ad_name','promo_name') comment='The name of the marketing campaign.',
		MARKETING_CAMPAIGN_METRICS.CATEGORY as CATEGORY with synonyms=('type','classification','group','label','section','genre','kind','class') comment='The category of the marketing campaign, which represents the product or service being promoted, such as a specific industry or product line, in this case, Fitness Wear.',
		MARKETING_CAMPAIGN_METRICS.DATE as DATE with synonyms=('day','timestamp','calendar_date','datestamp','calendar_day','entry_date') comment='Date on which the marketing campaign metrics were recorded.',
		PRODUCTS.CATEGORY as CATEGORY with synonyms=('type','classification','group','product_type','product_group','class','genre','kind','product_category') comment='The category of the product, which can be one of the following: Fitness Wear (apparel designed for athletic or fitness activities), Casual Wear (everyday clothing for general use), or Accessories (items that complement or enhance a product, such as hats, scarves, or bags).',
		PRODUCTS.PRODUCT_NAME as PRODUCT_NAME with synonyms=('item_name','product_title','item_title','product_description','product_label','item_label') comment='The name of the product being sold, such as a specific type of fitness equipment or accessory.'
	)
	with extension (CA='{"tables":[{"name":"MARKETING_CAMPAIGN_METRICS","dimensions":[{"name":"CAMPAIGN_NAME","sample_values":["Summer Fitness Campaign"]},{"name":"CATEGORY","sample_values":["Fitness Wear"]}],"facts":[{"name":"CLICKS","sample_values":["429","552","446"]},{"name":"IMPRESSIONS","sample_values":["10238","9962","7278"]}],"time_dimensions":[{"name":"DATE","sample_values":["2025-06-17","2025-06-15","2025-07-01"]}]},{"name":"PRODUCTS","dimensions":[{"name":"CATEGORY","sample_values":["Fitness Wear","Casual Wear","Accessories"]},{"name":"PRODUCT_NAME","sample_values":["Fitness Item 3","Accessories Item 7","Fitness Item 2"]}],"facts":[{"name":"PRODUCT_ID","sample_values":["29","1","2"]}]}]}');