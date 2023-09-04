/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.core.spatialPartitioning.quadtree;

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
