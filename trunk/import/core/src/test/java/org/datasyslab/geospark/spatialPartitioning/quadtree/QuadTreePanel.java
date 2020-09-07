/*
 * FILE: QuadTreePanel
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.spatialPartitioning.quadtree;

import org.apache.log4j.Logger;

import javax.swing.JFrame;
import javax.swing.JPanel;

import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

@SuppressWarnings("serial")
public class QuadTreePanel
        extends JPanel
        implements KeyListener, MouseListener
{

    protected static Logger log = Logger.getLogger(QuadTreePanel.class);

    /**
     * Create a new panel
     */
    protected QuadTreePanel()
    {

    }

    protected void setupGui()
    {
        JFrame f = new JFrame();
        f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        f.addKeyListener(this);
        f.addMouseListener(this);
        f.add(this);
        f.setLocationRelativeTo(null);
        f.setSize(700, 700);
        f.setVisible(true);
    }

    public void keyTyped(KeyEvent e)
    {
        // TODO Auto-generated method stub

    }

    public void keyPressed(KeyEvent e)
    {
        // TODO Auto-generated method stub

    }

    public void keyReleased(KeyEvent e)
    {

        if (e.getKeyCode() == 27) {
            System.exit(10);
        }
    }

    public void mousePressed(MouseEvent e)
    {
        // TODO Auto-generated method stub

    }

    public void mouseReleased(MouseEvent e)
    {
        // TODO Auto-generated method stub

    }

    public void mouseEntered(MouseEvent e)
    {
        // TODO Auto-generated method stub

    }

    public void mouseExited(MouseEvent e)
    {
        // TODO Auto-generated method stub

    }

    public void mouseClicked(MouseEvent e)
    {
        // TODO Auto-generated method stub

    }
}
