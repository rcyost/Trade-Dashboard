
import streamlit as st
import pandas as pd
import numpy as np
import os
from streamlit_d3graph import d3graph
from datetime import datetime


def app():
    # d3 = d3graph()
    # Load karate example
    # adjmat, df = d3.import_example('karate')
    # Process the adjacency matrix
    # d3.graph(adjmat)

    # Set node properties
    # hover = '\nId: ' + adjmat.columns.astype(str) +'\nDegree: ' + df['degree'].astype(str) + '\nLabel: ' + df['label'].values
    # hover = hover.values
    # label = df['label'].values

    # # Set node properties
    # d3.set_node_properties(label=label, hover=hover, color=label)

    # Plot
    # d3.show()

    data=pd.read_csv(os.path.join(r'C:\Users\yosty\Desktop\Desktop_Folder\14 - git\IMF-DOTS-Project\00-data\dots\clean\dotsTimeSeries.csv'))

    data['period']=pd.to_datetime(data['period'])

    st.sidebar.text("Always first day of the month")
    period=st.sidebar.date_input(
                            label='Period',
                            value=data['period'].min())

    datafilter=data[data['period']==period]

    datafilter=datafilter.pivot_table(index='ReferenceArea', columns='CounterpartReferenceArea', values='value')

    datafilter=datafilter.replace(np.nan, 0)
    datafilter=datafilter[datafilter.columns[datafilter.columns.isin(datafilter.index)]]

    datafilter.index=datafilter.columns


    # testdf=pd.DataFrame({
    # 'one':[1 for x in range(4)],
    # 'two':[1 for x in range(4)],
    # 'three':[1 for x in range(4)],
    # 'four':[1 for x in range(4)]})

    # l=300
    # keepL=[]
    # for i in range(l):
    #     keepL.append(pd.DataFrame({str(i):[1 for x in range(l)]}))


    # testdf=pd.concat(keepL, axis=1)

    # testdf.index=testdf.columns

    d3 = d3graph()
    d3.graph(datafilter)

    d3.show()






