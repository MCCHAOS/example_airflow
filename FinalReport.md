# Design Choices for Dashboard
I explored a few options for building the dashboard, including direct Python code or Jupyter notebooks using Plotly or other tools. But having a little experience with both Tableau and PowerBI, I chose to use PowerBI to implement the dashboard. 

I implemented several of the suggested metrics and made sure to allow for several interactions via the slicers and filters in PowerBI so I could explore the data.


# Key Insights
The most noteable insight is that the business does not suffer from large spikes due to seasonality or timing. There is some expected variability, but it is fairly smooth and most categories are similar in overall 'shap' but vary by their scale.
The majority of this business occurs in summer and spring and involves the categories of 'bed bath table', 'health beauty', and 'sports leisure'.
The vast majority of payments are made via credit card.


# Challenges
Some challenges encountered with the dashboard involved supporting different sorting patterns than default alphabetical. These were resolved by supplying custom sort order fields where needed. But for the broader project leading up to the dashboard the biggest challenge was data quality, both in data content and my understanding of each field. Careful review and exploration along with appropriate cleaning steps helped resolve most issues.


# Recommendations
The primary recommendation is to cater towards the existing larger segments of the business, mostly to avoid major impacts. Credit cards are the major payment vehicle, so maintain or potentially expand which credit cards are accepted if possible. Most orders are made between 10 a.m. and 10 p.m. so any adjustments for staffing could take this into account to match with the order volume.
Lastly, the weaker areas of the business could provide good targets for growth, such as focusing on trying to grow the fall season sales.