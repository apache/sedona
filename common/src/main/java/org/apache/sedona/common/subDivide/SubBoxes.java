package org.apache.sedona.common.subDivide;

public class SubBoxes {
    private SubDivideExtent subBox;

    private SubDivideExtent subBox2;

    public SubBoxes(SubDivideExtent subBox, SubDivideExtent subBox2) {
        this.subBox = subBox;
        this.subBox2 = subBox2;
    }

    public SubDivideExtent getSubBox() {
        return subBox;
    }

    public SubDivideExtent getSubBox2() {
        return subBox2;
    }
}
