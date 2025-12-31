package com.minis.snake;

import java.security.SecureRandom;
import java.util.Arrays;

public class PositionSelector {

    private final int[] positions;
    private final int[] index;
    private final int height;
    private final int width;
    private final SecureRandom random = new SecureRandom();
    private int limit;

    public PositionSelector(int height, int width) {
        this.height = height;
        this.width = width;
        limit = this.height * this.width - 1;
        positions = new int[limit + 1];
        index = new int[limit + 1];
        init();
    }

    public boolean occupy(int[] position) {
        int pos = position[0] * width + position[1];
        int where = index[pos];
        if (where > limit)
            return false;
        swapPos(where, limit);
        limit--;
        return true;
    }

    public boolean unoccupy(int[] position) {
        int pos = position[0] * width + position[1];
        int where = index[pos];
        if (where <= limit)
            return false;
        swapPos(where, limit + 1);
        limit++;
        return true;
    }

    public boolean checkUnoccupied(int[] position) {
        int pos = position[0] * width + position[1];
        int where = index[pos];
        return where <= limit;
    }

    public  void debug(int[] position) {
        int pos = position[0] * width + position[1];
        int where = index[pos];
        System.out.println("where is "+ where+" and limit is "+limit);
        System.out.println("positions:"+Arrays.toString(positions));
        System.out.println("index:"+Arrays.toString(index));
    }

    public int[] randomUnoccupiedPosition() {
        int position = random.nextInt(limit + 1);
        int[] result = new int[] { positions[position] / width, positions[position] % width };
        return result;
    }

    public void reset() {
        init();
    }

    private void init() {
        limit = this.height * this.width - 1;
        for (int i = 0; i <= limit; i++) {
            positions[i] = i;
            index[i] = i;
        }
    }

    private void swapPos(int pos1, int pos2) {
        index[positions[pos1]] = pos2;
        index[positions[pos2]] = pos1;
        int tmp = positions[pos1];
        positions[pos1] = positions[pos2];
        positions[pos2] = tmp;
    }

}
