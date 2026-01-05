package com.minis.snake;


import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Snake {
    public static final char PLUS = '+';
    public static final char DEAD = 'X';
    public static final char HEAD = 'H';
    public static final char BODY_V = '|';
    public static final char BODY_H = '-';
    public static final char TURN_UR = 'L';
    public static final char TURN_UL = 'J';
    public static final char TURN_DL = '7';
    public static final char TURN_DR = 'F';
    public static final char TAIL_U = '^';
    public static final char TAIL_D = 'v';
    public static final char TAIL_L = '<';
    public static final char TAIL_R = '>';
    public static final char BORDER = '#';
    private static final String SERIALIZATION_TOKEN = ";";
    private static final Set<Character> SNAKE_CHARS = new HashSet<>();

    static {
        SNAKE_CHARS.addAll(
                asList(TAIL_U, TAIL_D, TAIL_L, TAIL_R, BODY_H, BODY_V, TURN_DR, TURN_DL, TURN_UL, TURN_UR, HEAD));
    }

    private final int depth, width;
    private final LinkedList<int[]> snake = new LinkedList<>();
    private final PositionSelector positionSelector;
    private final char[][] board;
    private final BlockingQueue<Direction> moveBuffer = new LinkedBlockingDeque<>();
    private Direction lastDirection;
    private int[] apple = null;
    private String cachedSerial = "";
    private boolean isDirty = true;

    public Snake(int depth, int width) {
        this.depth = depth;
        this.width = width;
        board = new char[depth + 2][width + 2];
        positionSelector = new PositionSelector(depth, width);
        init();
    }

    public static boolean isSnake(char ch) {
        return SNAKE_CHARS.contains(ch);
    }

    public Move move() {
        DirectionBatch batch = consumeDirectionBuffer();
        return executeMoveBatch(batch);
    }

    private DirectionBatch consumeDirectionBuffer() {
        if (moveBuffer.isEmpty()) {
            return new DirectionBatch(lastDirection, 1);
        }

        Direction direction = moveBuffer.poll();
        int multiplier = 1;

        while (!moveBuffer.isEmpty() && moveBuffer.peek() == direction) {
            moveBuffer.poll();
            multiplier++;
        }
        if (multiplier > 5)
            multiplier = 5;
        return new DirectionBatch(direction, multiplier);
    }

    private Move executeMoveBatch(DirectionBatch batch) {
        int[] oldHead = snake.getLast();
        int[] currentHead = oldHead;
        Direction previousDirection = lastDirection;
        int eatenAt = 0;
        boolean scored = false;

        for (int step = 0; step < batch.multiplier; step++) {
            int stepsRemaining = batch.multiplier - step;
            MoveResult result = executeSingleMove(currentHead, batch.direction, previousDirection, stepsRemaining);

            if (!result.alive) {
                return new Move(false, batch.multiplier, false);
            }

            if (result.scored) {
                scored = true;
                eatenAt = step;
            }

            currentHead = result.newHead;
            previousDirection = batch.direction;
        }

        animateApple(oldHead, currentHead);
        lastDirection = previousDirection;

        System.out.println("Last move: multiplier: " + batch.multiplier + " scored: " + scored);
        sanityCheck();
        isDirty = true;
        return new Move(true, batch.multiplier - eatenAt, scored);
    }

    private MoveResult executeSingleMove(int[] currentHead, Direction direction, Direction previousDirection,
                                         int stepsRemaining) {
        int[] newHead = calculateNewPosition(currentHead, direction);

        if (isCollision(newHead)) {
            handleCollision(newHead, currentHead, direction, previousDirection);
            updateTailGraphic();
            return MoveResult.dead();
        }

        boolean scored = (board[newHead[0]][newHead[1]] == PLUS);

        occupyPosition(newHead);

        if (scored) {
            board[newHead[0]][newHead[1]] = (char) (stepsRemaining + 48);
            spawnNewApple();
        } else {
            removeTail();
            board[newHead[0]][newHead[1]] = HEAD;
        }

        snake.addLast(newHead);
        updateHeadGraphics(currentHead, direction, previousDirection);
        updateTailGraphic();

        return MoveResult.alive(newHead, scored);
    }

    private int[] calculateNewPosition(int[] position, Direction direction) {
        return new int[] {
                position[0] + direction.getDelta()[0],
                position[1] + direction.getDelta()[1]
        };
    }

    private boolean isCollision(int[] position) {
        if (position[0] < 1 || position[0] >= board.length - 1)
            return true;
        if (position[1] < 1 || position[1] >= board[position[0]].length - 1)
            return true;

        char cell = board[position[0]][position[1]];
        return isSnake(cell) || cell > 47; // Numbers indicating digestion
    }

    private void handleCollision(int[] newHead, int[] currentHead, Direction direction, Direction previousDirection) {
        board[newHead[0]][newHead[1]] = DEAD;
        int[] oldTail = snake.removeFirst();

        if (!(newHead[0] == oldTail[0] && newHead[1] == oldTail[1])) {
            clearPosition(oldTail);
        }

        updateHeadGraphics(currentHead, direction, previousDirection);
    }

    private void removeTail() {
        int[] oldTail = snake.removeFirst();
        clearPosition(oldTail);
    }

    private void spawnNewApple() {
        int[] newApple = positionSelector.randomUnoccupiedPosition();
        apple = toBoardCoordinates(newApple);
        board[apple[0]][apple[1]] = PLUS;
    }

    private void updateHeadGraphics(int[] position, Direction newDirection, Direction previousDirection) {
        if (board[position[0]][position[1]] == HEAD) {
            board[position[0]][position[1]] = calculateBodySegmentChar(newDirection, previousDirection);
        }
    }

    private char calculateBodySegmentChar(Direction currentDirection, Direction previousDirection) {
        if (currentDirection == previousDirection) {
            return isStraightVertical(currentDirection) ? BODY_V : BODY_H;
        }

        return calculateTurnChar(currentDirection, previousDirection);
    }

    private boolean isStraightVertical(Direction direction) {
        return direction == Direction.UP || direction == Direction.DOWN;
    }

    private char calculateTurnChar(Direction current, Direction previous) {
        // Previous direction is where we came FROM
        // Current direction is where we're going TO
        if (current == Direction.UP) {
            return previous == Direction.LEFT ? TURN_UR : TURN_UL;
        } else if (current == Direction.DOWN) {
            return previous == Direction.LEFT ? TURN_DR : TURN_DL;
        } else if (current == Direction.LEFT) {
            return previous == Direction.UP ? TURN_DL : TURN_UL;
        } else { // RIGHT
            return previous == Direction.UP ? TURN_DR : TURN_UR;
        }
    }

    private void updateTailGraphic() {
        if (snake.size() < 2)
            return;

        int[] tail = snake.getFirst();
        int[] neck = snake.get(1);

        // Skip if tail is currently digesting (has a number)
        char currentChar = board[tail[0]][tail[1]];
        if (currentChar >= '0' && currentChar <= '5') {
            return;
        }

        board[tail[0]][tail[1]] = calculateTailChar(tail, neck);
    }

    private char calculateTailChar(int[] tail, int[] neck) {
        if (neck[0] < tail[0])
            return TAIL_D;
        if (neck[0] > tail[0])
            return TAIL_U;
        if (neck[1] < tail[1])
            return TAIL_R;
        if (neck[1] > tail[1])
            return TAIL_L;
        return ' '; // Should not happen
    }

    private void animateApple(int[] oldHead, int[] currentHead) {
        int deltaX = currentHead[0] - oldHead[0];
        int deltaY = currentHead[1] - oldHead[1];

        int newX = apple[0] + sign(deltaX);
        int newY = apple[1];

        if (deltaX == 0) {
            newY += sign(deltaY);
        }

        newX = clampX(newX);
        newY = clampY(newX, newY);

        if (board[newX][newY] == ' ') {
            board[apple[0]][apple[1]] = ' ';
            board[newX][newY] = PLUS;
            apple[0] = newX;
            apple[1] = newY;
        }
    }

    private int sign(int value) {
        return Integer.compare(value, 0);
    }

    private int clampX(int x) {
        if (x < 1)
            return 2;
        return Math.min(x, board.length - 2);
    }

    private int clampY(int x, int y) {
        if (y < 1)
            return 2;
        return Math.min(y, board[x].length - 2);
    }

    // Position management helpers
    private void occupyPosition(int[] boardPos) {
        positionSelector.occupy(toSelectorCoordinates(boardPos));
    }

    private void clearPosition(int[] boardPos) {
        board[boardPos[0]][boardPos[1]] = ' ';
        positionSelector.unoccupy(toSelectorCoordinates(boardPos));
    }

    private int[] toSelectorCoordinates(int[] boardPos) {
        return new int[] { boardPos[0] - 1, boardPos[1] - 1 };
    }

    private int[] toBoardCoordinates(int[] selectorPos) {
        return new int[] { selectorPos[0] + 1, selectorPos[1] + 1 };
    }

    private void sanityCheck() {
        for (int i = 1; i < board.length - 1; i++) {
            for (int j = 1; j < board[i].length - 1; j++) {
                var position = new int[] { i - 1, j - 1 };
                var occupied = !positionSelector.checkUnoccupied(position);
                var ch = board[i][j];
                if (ch == ' ') {
                    if (occupied) {
                        System.out.println("Position " + i + ", " + j + " shouldnt be considered occupied");
                        board[i][j] = 'I';
                        System.out.println(print());
                        positionSelector.debug(position);
                        System.exit(1);
                    }
                } else if (SNAKE_CHARS.contains(ch)) {
                    if (!occupied) {
                        board[i][j] = 'I';
                        System.out.println(print());
                        System.out.println("Position " + i + ", " + j + " snake should be occupied.");
                        positionSelector.debug(position);
                        System.exit(1);
                    }
                } else if (ch == HEAD) {
                    if (!occupied) {
                        board[i][j] = 'I';
                        System.out.println(print());
                        System.out.println("Position " + i + ", " + j + " head should be occupied.");
                        positionSelector.debug(position);
                        System.exit(1);
                    }
                } else if (ch == PLUS) {
                    if (occupied) {
                        board[i][j] = 'I';
                        System.out.println(print());
                        System.out.println("Position " + i + ", " + j + " apple should not be occupied.");
                        positionSelector.debug(position);
                        System.exit(1);
                    }
                } else if (!occupied) {
                    board[i][j] = 'I';
                    System.out.println(print());
                    System.out.println("Position " + i + ", " + j + " char " + ch + " should be occupied.");
                    positionSelector.debug(position);
                    System.exit(1);
                }
            }
        }
    }

    public void reset() {
        positionSelector.reset();
        snake.clear();
        moveBuffer.clear();
        lastDirection = null;
        init();
    }

    private void init() {
        int[] head = new int[] { depth / 2, width / 2 };

        for (int i = 0; i < board.length; i++) {
            for (int j = 0; j < board[i].length; j++) {
                boolean isBorder = (i == 0 || i == board.length - 1 || j == 0 || j == board[i].length - 1);
                if (i == head[0] && j == head[1]) {
                    snake.addLast(new int[] { i, j });
                    board[i][j] = HEAD;
                    positionSelector.occupy(new int[] { i - 1, j - 1 });
                } else if (isBorder) {
                    board[i][j] = BORDER;
                } else {
                    board[i][j] = ' ';
                }
            }
        }
        var newApple = positionSelector.randomUnoccupiedPosition();
        apple = new int[] { newApple[0] + 1, newApple[1] + 1 };
        animateApple(head, head);
        right();
        isDirty = true;
    }

    public char[][] getBoard() {
        return board;
    }

    public List<Direction> getMoveBuffer() {
        return new ArrayList<>(moveBuffer);
    }

    public String print() {
        StringBuilder sb = new StringBuilder();
        for (char[] row : board) {
            sb.append('|');
            for (char cell : row) {
                sb.append(cell);
            }
            sb.append("|\n");
        }
        return sb.toString();
    }

    public void up() {
        moveBuffer.offer(Direction.UP);
    }

    public void down() {
        moveBuffer.offer(Direction.DOWN);
    }

    public void right() {
        moveBuffer.offer(Direction.RIGHT);
    }

    public void left() {
        moveBuffer.offer(Direction.LEFT);
    }

    public String getSerializedState() {
        if (!isDirty) {
            return cachedSerial;
        }

        StringBuilder sb = new StringBuilder();
        // Skip borders (index 0 and length-1)
        for (int i = 1; i < board.length - 1; i++) {
            for (int j = 1; j < board[i].length - 1; j++) {
                char c = board[i][j];
                if (c != ' ') {
                    if (sb.length() > 0)
                        sb.append(SERIALIZATION_TOKEN);
                    sb.append(i).append(",").append(j).append(",").append(c);
                }
            }
        }

        // Update Cache
        cachedSerial = sb.toString();
        isDirty = false; // Mark as clean
        return cachedSerial;
    }

    public void deserializeBoard(String state) {
        // 1. Reset Board (Clear everything except borders)
        // We assume init() was called once to set up borders
        for (int i = 1; i < board.length - 1; i++) {
            for (int j = 1; j < board[i].length - 1; j++) {
                board[i][j] = ' ';
            }
        }

        if (state == null || state.isEmpty())
            return;

        // 2. Parse the sparse list
        String[] atoms = state.split(SERIALIZATION_TOKEN);
        for (String atom : atoms) {
            String[] parts = atom.split(",");
            if (parts.length == 3) {
                try {
                    int r = Integer.parseInt(parts[0]);
                    int c = Integer.parseInt(parts[1]);
                    if (!parts[2].isEmpty()) {
                        board[r][c] = parts[2].charAt(0);
                    }
                } catch (Exception e) {
                    // ignore malformed atom!
                }
            }
        }
    }

    // Helper classes
    private static class DirectionBatch {
        final Direction direction;
        final int multiplier;

        DirectionBatch(Direction direction, int multiplier) {
            this.direction = direction;
            this.multiplier = multiplier;
        }
    }

    private static class MoveResult {
        final boolean alive;
        final int[] newHead;
        final boolean scored;

        private MoveResult(boolean alive, int[] newHead, boolean scored) {
            this.alive = alive;
            this.newHead = newHead;
            this.scored = scored;
        }

        static MoveResult alive(int[] newHead, boolean scored) {
            return new MoveResult(true, newHead, scored);
        }

        static MoveResult dead() {
            return new MoveResult(false, null, false);
        }
    }

    public static final class Move {
        private final boolean alive;
        private final int distance;
        private final boolean eaten;

        private Move(boolean alive, int distance, boolean eaten) {
            this.alive = alive;
            this.distance = distance;
            this.eaten = eaten;
        }

        public boolean isAlive() {
            return alive;
        }

        public boolean eaten() {
            return eaten;
        }

        public int distance() {
            return distance;
        }
    }
}