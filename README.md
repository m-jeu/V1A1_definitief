Team V1A1

Henry de Lange
Sjoerd Beetsma
Maarten de Jeu

Project Recommendation Engine:
De Op = Op Voordeelshop is een discount-(web)winkel, met een divers assortiment aan producten, van verzorgingsproducten tot elektronica. De Op = Op voordeelshop heeft als doelstelling de omzet per winkelwagentje te maximaliseren. Om te helpen deze doelstelling te realiseren op het web front, heeft de Op = Op voordeelshop het bedrijf Shopping Minds ingeschakeld, een bedrijf dat andere bedrijven helpt met het verbeteren met hun websites en webshops. Shopping minds doet dit bij de Op = Op Voordeelshop onder anderen door hun Data Management Platform (DMP) toe te passen op de webshop, en hier vervolgens een Recommendation Engine op te bouwen. De doelstelling van deze recommendation engine is niet om (potentiële) klanten producten aan te smeren, maar eerder om producten aan deze gebruikers aan te raden waar deze daadwerkelijk in geïnteresseerd zou zijn.

Om dit alles te bevorderen, zijn wij als ontwikkelaarsteam ingeschakeld door Shopping Minds om nieuwe ideeën voor algoritmes voor de recommendation engine te genereren. Door een aantal nieuwe ideeën voor recommendation-regels te onderzoeken, en deze in een testomgeving (lees: nep webshop), op basis van de dataset van de Op = Op Voordeelshop, te implementeren als Proof-of-Concept, hoopt Shopping Minds de daadwerkelijke recommendation-engine voor de Op = Op Voordeelshop te kunnen verbeteren.


directory layout:
/database: contains the scripts to convert MongoDB to psql, and a class to communicate with psql in PostgresDAO.
	mongo_to_pg.py converts MongoDB dataset to the relational database.
	MongodbDAO.py  contains functions to fetch data from MongoDB
	PostgresDAO.py contains class to communicate with psql.
/front_end: contains all the front_end files provided by Nick Roumpimper changes made to HUW.py and HUW_recommend.py to work with new recommendation rules.
	huw.py	front_end
	huw_recommendation.py front end recommendations
/interface:
	control_panel.py	simple ControlPanel type GUI to do tasks like regenerate the database, fill recommendation tables etc.
/recommendation_rules: contains all the code relating to the recommendation rules.
	/buying_power_recommendation: contains the code for budget preference recommendations.
		determine_user_price_preference.py
		frequently_combined_proid_sub_sub_category.py: used to calculate and store which product has been combined with which sub_sub_category most is historical orders
		subsubcat_price_information.py: used to calculate and store average price and standard deviation of each sub_sub_category from ordered products.
	freq_combined_sub_sub_category_recommendation.py
	popularity_norm__recommendation.py
	propositional_logic_recommendation.py
	simple_recommendation.py
	query_functions.py: premade functions to easily make certain queries
	statistics.py: used to calculate statistics


How to run:
First fill the PostgreSQL database
1. edit database/PostgresDAO.py to the correct psycopg2 connection credentials
2. run database/mongo_to_pg.py
Then run the required scripts to fill the recommendation tables in the PosstgreSQL database
3. run recommendation_rules/buying_power_recommendation/sub_sub_category_information.py
4. run recommendation_rules/buying_power_recommendation/determine_user_price_preference.py
5. run recommendation_rules/buying_power_recommendation/frequently_combined_proid_sub_sub_category
6. run recommendation_rules/freq_combined_sub_sub_category_recommendation.py
7. run recommendation_rules/popularity_norm_recommendation.py
8. run recommendation_rules/propositional_logic_recommendation.py

The recommendation engine is now ready to be used in the front-end by running huw_recommend.py and huw.py in front_end/
