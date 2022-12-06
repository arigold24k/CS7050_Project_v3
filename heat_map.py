import pandas as pd
import os
import plotly.express as px


# generates heatmaps of the crimes, requires a dataframe with latitude and longitude coordinates
# map_type: 'general' - generates 1 heatmap of all the data; 'monthly' - generates heatmap for each month Jan 2021-Oct 2022
# 'category' - generates separate heatmap for crime types (Auto Theft, Aggrivated Assault, Burglary, Homicide)
def generateHeatMap(dataframe, map_type='general'):
    df_raw_data_dropna = dataframe

    #create rounds lat and long values in order to create bins
    latToBin = lambda x: round(x,4)
    longToBin = lambda x: round(x,4)
    df_raw_data_dropna['lat'] = latToBin(df_raw_data_dropna.lat)
    df_raw_data_dropna['long'] = longToBin(df_raw_data_dropna.long)

    # generate a heat map of all crime and all dates
    if map_type == 'general':
        condensedMapDF = df_raw_data_dropna.loc[:, ['lat', 'long']].copy()
        condensedMapDF['count'] = 1
        condensedMapDFcounts = condensedMapDF.groupby(['lat', 'long'])['count'].sum().reset_index()
        fig = px.density_mapbox(condensedMapDFcounts, lat='lat', lon='long', z='count', mapbox_style="carto-positron", radius=50, zoom=11, title=f'All crime from Jan 2021 - Nov 2022')
        fig.show()

    # generates a heat map for each month between Jan 2021 - Oct 2022
    elif map_type == 'monthly':
        df_raw_data_dropna['month'] = df_raw_data_dropna['occur_date'].dt.month.astype('int32')
        df_raw_data_dropna['year'] = df_raw_data_dropna['occur_date'].dt.year.astype('int32')

        years = [2021, 2022]
        months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

        for j in range(2):
            #queries data to get subset based on year
            year = years[j]
            dataSubset1 = df_raw_data_dropna.query('year == @year')
            for i in range(1,13):
                if j == 1 and i == 11:
                    break

                # queries data to get subset based on month
                dataSubset2 = dataSubset1.query('month == @i')

                # generates heatmap based on current subset
                condensedMapDF = dataSubset2.loc[:, ['lat', 'long']].copy()
                condensedMapDF['count'] = 1
                condensedMapDFcounts = condensedMapDF.groupby(['lat', 'long'])['count'].sum().reset_index()
                fig = px.density_mapbox(condensedMapDFcounts, lat='lat', lon='long', z='count', mapbox_style="carto-positron", radius=50, zoom=11, range_color=[0,20], title=f'All crime occurred in {months[i-1]} {year}')
                fig.show()

    # generates a heatmap for several different crime types
    elif map_type == 'category':
        crimeList = ['AUTO THEFT', 'AGG ASSAULT', 'BURGLARY', 'HOMICIDE']
        for crime in crimeList:
            # queries data based on crime type
            dataSubset = df_raw_data_dropna.query('UC2_Literal == @crime')

            # generates heatmap based on current subset
            condensedMapDF = dataSubset.loc[:, ['lat', 'long']].copy()
            condensedMapDF['count'] = 1
            condensedMapDFcounts = condensedMapDF.groupby(['lat', 'long'])['count'].sum().reset_index()
            max = condensedMapDFcounts.loc[:,'count'].max()
            fig = px.density_mapbox(condensedMapDFcounts, lat='lat', lon='long', z='count', mapbox_style="carto-positron", radius=50, range_color=[0,max], zoom=11, title=f'All {crime} from Jan 2021 - Nov 2022')
            fig.show()

    return None



def main():
    basepath = os.path.dirname(__file__)
    transformed_data = pd.read_csv(os.path.join(basepath, 'DataFiles', 'Transformed_data.csv'), skipinitialspace = True)

    pd.set_option('display.max_columns', 50)
    pd.set_option('display.width', 1500)

    # run exploratory data analysis
    generateHeatMap(transformed_data, map_type='general')


if __name__ == '__main__':
    main()
