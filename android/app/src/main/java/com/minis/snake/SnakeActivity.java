package com.minis.snake;

import android.os.Bundle;

import androidx.appcompat.app.AppCompatActivity;

import com.minis.Minis;
import com.minis.R;

public class SnakeActivity extends AppCompatActivity {

    private PlayManager playManager;
    private Minis minis;
    private String dbPath;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_snake);

        dbPath = getFilesDir().getAbsolutePath() + "/snake.db";
        minis = new Minis();
        minis.load(dbPath);
        // 1. Init Logic
        playManager = new PlayManager(this, minis);

        // 2. Setup View
        SnakeView snakeView = findViewById(R.id.snakeView);
        snakeView.setPlayManager(playManager);

        // 3. Connect Buttons (This is your new KeyHandler)
        findViewById(R.id.btnUp).setOnClickListener(v -> playManager.up());
        findViewById(R.id.btnDown).setOnClickListener(v -> playManager.down());
        findViewById(R.id.btnLeft).setOnClickListener(v -> playManager.left());
        findViewById(R.id.btnRight).setOnClickListener(v -> playManager.right());

        findViewById(R.id.btnReset).setOnClickListener(v -> playManager.maybeReset());

    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        if (minis != null) {
            minis.save(dbPath);
            minis.close();
        }
    }
}
