from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import numpy as np

class PointToPointRegressor:

    def __init__(self):
        self.linear_models = []  # linear regression models between each pair of consecutive points
        self.exponential_models = []  # exponential models between each pair of consecutive points
    
    def fit(self, X, y):
        self.X = X
        self.y = y
        n = len(X)
        self.points = []  # list of (x, y) points in the data
        for i in range(n - 1):
            x1, y1 = X[i], y[i]
            x2, y2 = X[i+1], y[i+1]
            self.points.append((x1, y1))
        self.points.append((X[-1], y[-1]))  # add the last point

        self.points = np.array(self.points)
        points_indices = np.argsort(self.points[:,0])
        self.points = self.points[points_indices]
        for i in range(len(self.points) - 1):
            x1, y1 = self.points[i]
            x2, y2 = self.points[i+1]
            model = LinearRegression().fit([[x1], [x2]], [y1, y2])
            self.linear_models.append(model)
            
    def predict(self, X_test):

        if X_test < self.points[:,0].min():
            return self.linear_models[0].predict(np.vstack([X_test]).T)[0]
        if X_test > self.points[:,0].max():
            return self.linear_models[-1].predict(np.vstack([X_test]).T)[0]
        for i in range(len(self.points) - 1):
            x1, y1 = self.points[i]
            x2, y2 = self.points[i+1]
            if x1 <= X_test <= x2:
                model = self.linear_models[i]
                X_test_2d = np.vstack([X_test]).T
                return model.predict(X_test_2d)[0]
        
        # If X_test is outside the range of the data, return the y-value of the last point
        
    
    def plot(self):
        plt.scatter(*zip(*self.points))
        X_fit, y_fit = [], []
        for i in range(len(self.points) - 1):
            x1, y1 = self.points[i]
            x2, y2 = self.points[i+1]
            model = self.linear_models[i]
            X_fit.append(np.linspace(x1, x2, 100))
            y_fit.append(model.predict(np.vstack([X_fit[-1]]).T))
        X_fit = np.concatenate(X_fit)
        y_fit = np.concatenate(y_fit)
        #Add a title and axis labeels
        plt.title('Point To Point Regression Between Seconds and View Through Rate %')
        plt.xlabel('Seconds')
        plt.ylabel('VTR')
        plt.plot(X_fit, y_fit)
        plt.show()