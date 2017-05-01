/**
 * FILE: VectorOverlayOperator.java
 * PATH: org.datasyslab.babylon.core.VectorOverlayOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class VectorOverlayOperator.
 */
public class VectorOverlayOperator {
    
    /** The back vector image. */
    public List<String> backVectorImage=null;

    /** The distributed back vector image. */
    public JavaPairRDD<Integer,String> distributedBackVectorImage=null;

    /** The generate distributed image. */
    public boolean generateDistributedImage=false;

    /**
     * Instantiates a new vector overlay operator.
     *
     * @param distributedBackImage the distributed back image
     */
    public VectorOverlayOperator(JavaPairRDD<Integer,String> distributedBackImage)
    {
        this.distributedBackVectorImage = distributedBackImage;
        this.generateDistributedImage=true;
    }

    /**
     * Instantiates a new vector overlay operator.
     *
     * @param backVectorImage the back vector image
     */
    public VectorOverlayOperator(List<String> backVectorImage)
    {
        this.backVectorImage = backVectorImage;
        this.generateDistributedImage = false;
    }

    /**
     * Join image.
     *
     * @param distributedFontImage the distributed font image
     * @return true, if successful
     * @throws Exception the exception
     */
    public boolean JoinImage(JavaPairRDD<Integer,String> distributedFontImage) throws Exception
    {
        if (this.generateDistributedImage==false)
        {
            throw new Exception("[OverlayOperator][JoinImage] The back image is not distributed. Please don't use distributed format.");
        }
        // Prune SVG header and footer because we only need one header and footer per SVG even if we merge two SVG images.
        JavaPairRDD<Integer,String> distributedFontImageNoHeaderFooter = distributedFontImage.filter(new Function<Tuple2<Integer,String>,Boolean>()
        {

            @Override
            public Boolean call(Tuple2<Integer, String> vectorObject) throws Exception {
                // Check whether the vectorObject's key is 1. 1 means this object is SVG body.
                // 0 means this object is SVG header, 2 means this object is SVG footer.
                if(vectorObject._1 != 1)
                {
                    return false;
                }
                return true;
            }
        });
        this.distributedBackVectorImage = this.distributedBackVectorImage.union(distributedFontImageNoHeaderFooter);
        this.distributedBackVectorImage=this.distributedBackVectorImage.sortByKey();
        return true;
    }

    /**
     * Join image.
     *
     * @param frontVectorImage the front vector image
     * @return true, if successful
     * @throws Exception the exception
     */
    public boolean JoinImage(List<String> frontVectorImage) throws Exception
    {
        if (this.generateDistributedImage==true)
        {
            throw new Exception("[OverlayOperator][JoinImage] The back image is distributed. Please don't use centralized format.");
        }
        // Merge two SVG images. Skip the first element and last element because they are SVG image header and footer.
        List<String> copyOf= new ArrayList<String>();
        for(int i=0;i<this.backVectorImage.size()-1;i++)
        {
            copyOf.add(this.backVectorImage.get(i));
        }
        for(int i=1;i<frontVectorImage.size()-1;i++)
        {
            copyOf.add(frontVectorImage.get(i));
        }
        copyOf.add(this.backVectorImage.get(this.backVectorImage.size()-1));
        this.backVectorImage = copyOf;
        return true;
    }
}
