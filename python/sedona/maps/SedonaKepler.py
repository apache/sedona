from keplergl import KeplerGl
import geopandas as gpd



class SedonaKepler:

    @classmethod
    def create_map(cls, df=None, name="unnamed", geometry_col="geometry", config=None):
        """
        Creates a map visualization using kepler, optionally taking a sedona dataFrame as data input
        :param df: [Optional] SedonaDataFrame to plot on the map
        :param name: [Optional] Name to be associated with the given dataframe, if a df is passed with no name, a default name of 'unnamed' is set for it.
        :param geometry_col: [Optional] Custom name of geometry column in the sedona data frame,
                            if no name is provided, it is assumed that the column has the default name 'geometry'.
        :param config: [Optional] A map config to be applied to the rendered map
        :return: A map object
        """
        if df is not None:
            geoDf = SedonaKepler._convertToGdf(df, geometry_col)
            dataDict = {name: geoDf}
            map = KeplerGl(data=dataDict)
        else:
            map = KeplerGl()

        if config is not None:
            map.config = config

        return map

    @classmethod
    def add_df(cls, map, df, name="unnamed", geometry_col="geometry"):
        """
        Adds a SedonaDataFrame to a given map object.
        :param map: Map object to add SedonaDataFrame to
        :param df: SedonaDataFrame to add
        :param name: [Optional] Name to assign to the dataframe, default name assigned is 'unnamed'
        :param geometry_col: [Optional] Custom name of geometry_column if any, if no name is provided, a default name of 'geometry' is assumed.
        :return: Does not return anything, adds df directly to the given map object
        """
        geoDf = SedonaKepler._convertToGdf(df, geometry_col)
        map.add_data(geoDf, name=name)


    @classmethod
    def _convertToGdf(cls, df, geometry_col="geometry"):
        """
        Converts a SedonaDataFrame to a GeoPandasDataFrame and also renames geometry column to a standard name of 'geometry'
        :param df: SedonaDataFrame to convert
        :param geometry_col: [Optional]
        :return:
        """
        pandasDf = df.toPandas()
        geoDf = gpd.GeoDataFrame(pandasDf, geometry=geometry_col)
        if geometry_col != "geometry":
            geoDf = geoDf.rename(columns={geometry_col: "geometry"})
        return geoDf

