-- Registering the jar file
REGISTER ./pigPageLoad.jar

-- Reading the input file using the PageRankLoad function
records = LOAD '/user/cloudera/PageRank/input.xml' USING 
			com.example.pigpagerank.PageRankLoad() AS 
			(page:chararray,links:bag{});

-- Filtering the records so that the page is not null
records = FILTER records BY page IS NOT NULL;

-- Generate the number of links and scores to give
no_of_links_points_per_page = FOREACH records GENERATE page,
								COUNT(links) AS no_of_links,
								(float)1/COUNT(links) AS points;
							 	
-- Flattening the bag links							 	
page_link_mapping = FOREACH records GENERATE page AS page,
						FLATTEN(links) AS link;							 	
			
-- removing the links not present in dataset
remove_links_not_pages = JOIN no_of_links_points_per_page BY page,
							page_link_mapping BY link;
only_good_pages = FOREACH remove_links_not_pages GENERATE 
							page_link_mapping::page AS page,
							 page_link_mapping::link AS link;
							
-- Join to get the score against each link
joinResult = JOIN no_of_links_points_per_page BY page,only_good_pages BY page;												
							
-- Grouping the join result by link
groupResult = GROUP joinResult BY only_good_pages::link;

-- Aggregating the 'points' column to get rank
result = FOREACH groupResult GENERATE group AS page,
			SUM(joinResult.no_of_links_points_per_page::points) AS 
			rank;

-- ordering the result by rank
ordered = ORDER result BY rank DESC;
			
-- Storing the Result
STORE ordered INTO './Output';			
