"""Shared Streamlit rendering helpers."""

import streamlit as st


def render_yearly_table(df_yearly):
    """Render the year-over-year summary table used on Overview and Rating Trends."""
    df_display = df_yearly.sort_values("year", ascending=False).copy()
    df_display.columns = [
        "Year", "Total Ratings", "Unique Users", "Unique Movies",
        "Avg Rating", "Late Arrivals", "Late Arrival %",
    ]

    max_ratings = df_display["Total Ratings"].max()

    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Year": st.column_config.NumberColumn("Year", format="%d"),
            "Total Ratings": st.column_config.ProgressColumn(
                "Total Ratings",
                min_value=0,
                max_value=int(max_ratings),
                format="%d",
            ),
            "Unique Users": st.column_config.NumberColumn("Unique Users", format="%d"),
            "Unique Movies": st.column_config.NumberColumn("Unique Movies", format="%d"),
            "Avg Rating": st.column_config.NumberColumn("Avg Rating", format="%.2f"),
            "Late Arrivals": st.column_config.NumberColumn("Late Arrivals", format="%d"),
            "Late Arrival %": st.column_config.NumberColumn("Late Arrival %", format="%.3f%%"),
        },
    )
