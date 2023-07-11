from keplergl import KeplerGl

class SedonaKepler:

    @classmethod
    def createMap(cls, df=None, name=None, config=None):
        if df is not None:
            dfName = 'unnamed'
            if name is not None:
                dfName = name
            dataDict = {dfName: df}
            map = KeplerGl(data=dataDict)
        else:
            map = KeplerGl()

        if config is not None:
            map.config = config

        return map

