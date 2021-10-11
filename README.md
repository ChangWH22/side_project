# side_project

## regression
對PM2.5資料進行預測，使用線性回歸方式搭配gradient descent 尋找最佳解<br>
比較 adagrand 與 SGD；結果 Adagrand 可以比SGD更快使模型收斂<br>
比較 normalized 跟原始資料；結果經過Normalized 的資料比什麼都不做會來的更早收斂


## classification
loss function 使用cross entropy進行分類任務 ，使用gradient descent 更新權重尋找最佳解<br>
在gradient descent 處使用mini batch training 並且學習率隨更新次數越變越小
