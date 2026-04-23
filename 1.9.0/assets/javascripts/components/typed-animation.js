import Typewriter from 'typewriter-effect/dist/core';

export const typedAnimation = () => {
  const container = document.querySelector('.typed');
  const stringsContainer = document.querySelectorAll('.typed-strings p');

  if (container && stringsContainer.length) {
    const stringsArray = Array.from(stringsContainer).map((el) =>
      el.textContent.trim(),
    );

    const typewriter = new Typewriter(container, {
      loop: true,
      delay: 95, // typing speed
      deleteSpeed: 65, // deleting speed
    });

    stringsArray.forEach((text) => {
      typewriter
        .typeString(text)
        .pauseFor(500) // no pause after typing
        .deleteAll();
    });

    typewriter.start();
  }
};
