SedonaKepler offers a number of APIs which aid in quick and interactive visualization of a geospatial data in a Jupyter notebook/lab environment.

Inorder to start using SedonaKepler, simply import Sedona using:

```python
from sedona.spark import *
```

Alternatively it can also be imported using:

```python
from sedona.maps.SedonaKepler import SedonaKepler
```

Following are details on all the APIs exposed via SedonaKepler:

### **Creating a map object using SedonaKepler.create_map**

SedonaKepler exposes a create_map API with the following signature:

```python
create_map(df: SedonaDataFrame=None, name: str='unnamed', config: dict=None) -> map
```

The parameter 'name' is used to associate the passed SedonaDataFrame in the map object and any config applied to the map is linked to this name. It is recommended you pass a unique identifier to the dataframe here.

If no SedonaDataFrame object is passed, an empty map (with config applied if passed) is returned. A SedonaDataFrame can be added later using the method `add_df`

A map config can be passed optionally to apply pre-apply customizations to the map.

!!!Note
    The map config references every customization with the name assigned to the SedonaDataFrame being displayed, if there is a mismatch in the name, the config will not be applied to the map object.

!!! abstract "Example usage (Referenced from Sedona Jupyter examples)"

	=== "Python"
		```python
		map = SedonaKepler.create_map(df=groupedresult, name="AirportCount")
		map
		```

### **Adding SedonaDataFrame to a map object using SedonaKepler.add_df**

SedonaKepler exposes an add_df API with the following signature:

```python
add_df(map, df: SedonaDataFrame, name: str='unnamed')
```

This API can be used to add a SedonaDataFrame to an already created map object. The map object passed is directly mutated and nothing is returned.

The parameters name has the same conditions as 'create_map'

!!!Tip
    This method can be used to add multiple dataframes to a map object to be able to visualize them together.

!!! abstract "Example usage (Referenced from Sedona Jupyter examples)"
    === "Python"
    ```python
    map = SedonaKepler.create_map()
    SedonaKepler.add_df(map, groupedresult, name="AirportCount")
    map
    ```

### **Setting a config via the map**

A map rendered by accessing the map object created by SedonaKepler includes a config panel which can be used to customize the map

### **Saving and setting config**

A map object's current config can be accessed by accessing its 'config' attribute like `map.config`. This config can be saved for future use or use across notebooks if the exact same map is to be rendered every time.

!!!Note
    The map config references each applied customization with the name given to the dataframe and hence will work only on maps with the same name of dataframe supplied.
    For more details refer to keplerGl documentation [here](https://docs.kepler.gl/docs/keplergl-jupyter#6.-match-config-with-data)
