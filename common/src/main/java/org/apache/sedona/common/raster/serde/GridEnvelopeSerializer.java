/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.common.raster.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.geotools.coverage.grid.GridEnvelope2D;

public class GridEnvelopeSerializer extends Serializer<GridEnvelope2D> {
    @Override
    public void write(Kryo kryo, Output output, GridEnvelope2D object) {
        output.writeInt(object.width);
        output.writeInt(object.height);
        output.writeInt(object.x);
        output.writeInt(object.y);
    }

    @Override
    public GridEnvelope2D read(Kryo kryo, Input input, Class<GridEnvelope2D> type) {
        int width = input.readInt();
        int height = input.readInt();
        int x = input.readInt();
        int y = input.readInt();
        return new GridEnvelope2D(x, y, width, height);
    }
}
