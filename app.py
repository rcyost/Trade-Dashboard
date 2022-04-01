import os
import streamlit as st
from multipage import MultiPage

from pages import series, network


app = MultiPage()

app.add_page("Network", network.app)
app.add_page("Series", series.app)




app.run()

