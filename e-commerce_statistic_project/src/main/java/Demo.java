import javax.swing.*;
import java.util.*;

/**
 * 给出二维平面上的n个点，求最多有多少点在同一条直线上。
 * 样例
 * 给出4个点：(1, 2), (3, 6), (0, 0), (1, 3)。
 * 一条直线上的点最多有3个。
 *
 * @author Dell
 */
class Point {
    public int x;
    public int y;

    public Point() {
        x = 0;
        y = 0;
    }

    public Point(int a, int b) {
        x = a;
        y = b;
    }

    @Override
    public String toString() {
        return "Point{" +
                "x=" + x +
                ", y=" + y +
                '}';
    }
}

class Test168 {
    public static int maxPoints(Point[] points) {
        if (points == null || points.length == 0)
            return 0;
        int max = 1;
        for (int i = 0; i < points.length; i++) {
            Map<Double, Integer> map = new HashMap<>();
            int tempmax = 1;
            int repeatnumber = 0;
            double k = 0;
            for (int j = i + 1; j < points.length; j++) {
                if (points[j].x - points[i].x == 0 && points[j].y - points[i].y == 0) {
                    repeatnumber++;
                    continue;
                } else if (points[j].x - points[i].x != 0) {
                    k = (double) (points[j].y - points[i].y) / (double) (points[j].x - points[i].x);
                } else {
                    k = (double) Integer.MIN_VALUE;
                }
                int num = 2;
                if (map.containsKey(k)) {
                    num = map.get(k);
                    map.put(k, ++num);
                } else {
                    map.put(k, num);
                }
                tempmax = Math.max(tempmax, num);
            }
            max = Math.max(max, tempmax + repeatnumber);
        }
        return max;
    }


    public static void main(String[] args) {
        int n = 4;
        List<Integer> integers = new ArrayList<>();
        integers.add(1);
        integers.add(2);
        integers.add(4);
        integers.add(6);


//        System.out.println(integers);
        Point[] p = new Point[n];
        for (int i = 0; i < n; i++) {
            System.out.println(i);
            p[i] = new Point(i, integers.get(i));
        }

        for (Point point : p) {
            System.out.println(point);
        }

        System.out.println(n -maxPoints(p));

    }

}