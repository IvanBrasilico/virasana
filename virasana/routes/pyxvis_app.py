# import pyxviz
from ajna_commons.utils import ImgEnhance
from ajna_commons.utils.images import PIL_tobytes
from flask import request, Response
from flask_login import login_required
from virasana.views import get_image


def configure(app):
    """Configura rotas para bagagem."""

    @app.route('/image_xvis', methods=['GET'])
    @login_required
    def image_xvis():
        _id = request.args.get('_id')
        n = request.args.get('n', 0)
        cutoff = request.args.get('cutoff', '10')
        equalize = request.args.get('equalize', False) == 'True'
        colorize = request.args.get('colorize', False) == 'True'
        cv2 = request.args.get('cv2', False) == 'True'
        image = get_image(_id, n, pil=True)
        if image:
            cutoff = int(cutoff)
            image = ImgEnhance.autocontrast(image, cutoff=cutoff,
                                            colorize=colorize, equalize=equalize,
                                            cv2=cv2)
            image = PIL_tobytes(image)
            return Response(response=image, mimetype='image/jpeg')
        return 'Sem imagem'
