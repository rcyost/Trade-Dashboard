

#%%
from streamlit_d3graph import d3graph

d3 = d3graph()
# Load karate example
adjmat, df = d3.import_example('karate')
# Process the adjacency matrix
d3.graph(adjmat)

# Set node properties
hover = '\nId: ' + adjmat.columns.astype(str) +'\nDegree: ' + df['degree'].astype(str) + '\nLabel: ' + df['label'].values
hover = hover.values
label = df['label'].values

# Set node properties
d3.set_node_properties(label=label, hover=hover, color=label)

# Plot
d3.show()