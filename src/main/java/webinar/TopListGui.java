package webinar;

import com.hazelcast.core.IMap;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.util.List;

import static java.awt.EventQueue.invokeLater;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toMap;
import static webinar.Stash.PUBLISH_KEY;

class TopListGui {
    private static final int WINDOW_X = 700;
    private static final int WINDOW_Y = 200;
    private static final int WINDOW_WIDTH = 300;
    private static final int WINDOW_HEIGHT = 350;

    private final IMap<Object, List<String>> topList;
    private Timer timer;
    private JFrame frame;

    TopListGui(IMap<Object, List<String>> topList) {
        this.topList = topList;
        invokeLater(this::buildFrame);
    }

    void shutdown() {
        timer.stop();
        frame.dispose();
    }

    private void buildFrame() {
        frame = new JFrame();
        frame.setBackground(Color.WHITE);
        frame.setTitle("Hazelcast Jet - Trending Words in Tweets");
        frame.setBounds(WINDOW_X, WINDOW_Y, WINDOW_WIDTH, WINDOW_HEIGHT);
        frame.setLayout(new BorderLayout());
        final JPanel mainPanel = new JPanel();
        mainPanel.setBackground(Color.WHITE);
        mainPanel.setLayout(new BorderLayout(10, 10));
        mainPanel.setBorder(new EmptyBorder(20, 120, 20, 20));
        frame.add(mainPanel);
        final JTextArea output = new JTextArea();
        mainPanel.add(output, BorderLayout.CENTER);
        output.setFont(output.getFont().deriveFont(18f));
        timer = new Timer(100, e -> output.setText(topList.get(PUBLISH_KEY).stream().collect(joining("\n"))));
        timer.start();
        frame.setVisible(true);
    }
}
