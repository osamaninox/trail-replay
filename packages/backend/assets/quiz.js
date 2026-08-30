/* Reusable quiz widget — trail-replay teaching workspace.
 *
 * Usage in a lesson:
 *   <div class="quiz">
 *     <div class="quiz-q">
 *       <p class="quiz-prompt">Question text?</p>
 *       <ul class="quiz-options">
 *         <li data-correct="false">Wrong option</li>
 *         <li data-correct="true">Right option</li>
 *       </ul>
 *       <p class="quiz-explain" hidden>Explanation shown after answering.</p>
 *     </div>
 *     ...more .quiz-q blocks...
 *   </div>
 *
 * Behaviour: click an option -> immediate feedback, explanation reveals,
 * question locks. A running score appears once all questions are answered.
 */
document.addEventListener("DOMContentLoaded", function () {
  document.querySelectorAll(".quiz").forEach(function (quiz) {
    var questions = Array.prototype.slice.call(quiz.querySelectorAll(".quiz-q"));
    var answered = 0;
    var correct = 0;

    questions.forEach(function (q) {
      var options = q.querySelectorAll(".quiz-options li");
      options.forEach(function (opt) {
        opt.addEventListener("click", function () {
          if (q.classList.contains("answered")) return;
          q.classList.add("answered");

          var isCorrect = opt.dataset.correct === "true";
          opt.classList.add(isCorrect ? "picked-correct" : "picked-wrong");

          if (!isCorrect) {
            options.forEach(function (o) {
              if (o.dataset.correct === "true") o.classList.add("reveal-correct");
            });
          } else {
            correct += 1;
          }
          answered += 1;

          var explain = q.querySelector(".quiz-explain");
          if (explain) explain.hidden = false;

          if (answered === questions.length) {
            var score = document.createElement("p");
            score.className = "quiz-score";
            score.textContent =
              "Score: " + correct + " / " + questions.length +
              (correct === questions.length
                ? " — concept secured."
                : " — worth re-reading the sections for the missed ones.");
            quiz.appendChild(score);
          }
        });
      });
    });
  });
});
